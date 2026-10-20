"""A WordPress-shaped fake with chaos: just enough of the WP REST API for the publisher, for many sites at once.

    python fake_platform.py      # serves http://127.0.0.1:8081/s/<site>/wp-json/wp/v2/...

Any request may fail with a 500 before doing anything, do the work and *then* fail with a 500, or do the work
and hang past the client's timeout (the nasty case: the caller can't tell whether it happened). Each site also
rate-limits with 429 + Retry-After (seconds or an HTTP-date), rejects bad credentials with 401, and its search
lags behind writes like a real replica or cache would. Slugs follow WP's rules: drafts may have none, and
publishing dedupes against every other non-trashed post, drafts included (`slug` -> `slug-2`).
GET /stats lists, per article marker, the slugs of its live posts and how many drafts are left.
"""
import base64
import json
import os
import random
import re
import threading
import time
from collections import Counter, defaultdict
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CHAOS = float(os.environ.get("CHAOS", 0.1))       # share of requests that misbehave
HANG = float(os.environ.get("HANG", 2))           # longer than the publisher's HTTP_TIMEOUT
RATE, BURST = float(os.environ.get("SITE_RATE", 10)), 3
SEARCH_LAG = float(os.environ.get("SEARCH_LAG", 2))
NO_TRASH = os.environ.get("NO_TRASH") == "1"      # like EMPTY_TRASH_DAYS=0: DELETE without force is a 501
GOOD_AUTH = "Basic " + base64.b64encode(b"ryze:secret").decode()
MARKER = re.compile(r"ryze:([0-9a-f-]{36})")

lock = threading.Lock()
posts, media, buckets = {}, [], {}


def allowed(site):
    tokens, at = buckets.get(site, (BURST, time.monotonic()))
    now = time.monotonic()
    tokens = min(BURST, tokens + (now - at) * RATE)
    buckets[site] = (tokens - (tokens >= 1), now)
    return tokens >= 1


def unique_slug(site, slug, pid):
    taken = {p["slug"] for p in posts.values() if p["site"] == site and p["id"] != pid}
    n, candidate = 2, slug
    while candidate in taken:
        candidate, n = f"{slug}-{n}", n + 1
    return candidate


def apply(method, site, kind, pid, query, body):
    if kind == "media":
        media.append(site)
        return {"id": len(media)}
    if method == "GET":
        statuses = query.get("status", ["publish"])[0].split(",")
        return [p for p in posts.values() if p["site"] == site and query["search"][0] in p["content"]["raw"]
                and p["status"] in statuses and p["created"] < time.time() - SEARCH_LAG]
    post = posts.get(pid)
    if method == "DELETE":  # no ?force: move to trash, and free the slug the way WP does
        post["status"], post["slug"] = "trash", post["slug"] and post["slug"] + "__trashed"
        return post
    if post:
        post["status"] = body.get("status", post["status"])
        if post["status"] == "publish":
            post["slug"] = unique_slug(site, body.get("slug") or post["slug"], pid)
        post["link"] = f"/s/{site}/{post['slug']}/"
        return post
    pid = len(posts) + 1
    posts[pid] = {"id": pid, "site": site, "slug": body.get("slug", ""), "status": body["status"],
                  "created": time.time(), "content": {"raw": body["content"]}, "link": f"/s/{site}/?p={pid}"}
    return posts[pid]


def stats():
    live, drafts = defaultdict(list), Counter()
    for p in posts.values():
        for uid in MARKER.findall(p["content"]["raw"]):
            if p["status"] == "publish":
                live[uid].append(p["slug"])
            elif p["status"] == "draft":
                drafts[uid] += 1
    return {"live": live, "drafts": drafts, "posts": len(posts), "media": len(media),
            "trashed": sum(p["status"] == "trash" for p in posts.values())}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")

    def route(self, method):
        url = urlparse(self.path)
        if url.path == "/stats":
            with lock:
                return self.reply(200, stats())
        if url.path == "/cdn/cover.jpg":
            return self.reply(200, b"\xff\xd8\xff not really a jpeg")
        m = re.fullmatch(r"/s/(\d+)/wp-json/wp/v2/(media|posts)(?:/(\d+))?", url.path)
        if not m:
            return self.reply(404, {"code": "rest_no_route"})
        site, kind, pid = m[1], m[2], m[3] and int(m[3])
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.loads(raw) if kind == "posts" and raw else {}
        if self.headers.get("Authorization") != GOOD_AUTH:
            return self.reply(401, {"code": "rest_not_logged_in"})
        with lock:
            if not allowed(site):  # real servers send either form
                after = random.choice(["1", formatdate(time.time() + 1, usegmt=True)])
                return self.reply(429, {"code": "too_many_requests"}, {"Retry-After": after})
            if pid and pid not in posts:
                return self.reply(404, {"code": "rest_post_invalid_id"})
            if method == "DELETE" and NO_TRASH:
                return self.reply(501, {"code": "rest_trash_not_supported"})
            if method == "DELETE" and posts[pid]["status"] == "trash":
                return self.reply(410, {"code": "rest_already_trashed"})
        roll = random.random()
        if roll < CHAOS / 3:
            return self.reply(500, {"code": "internal_server_error"})  # failed, nothing happened
        with lock:
            result = apply(method, site, kind, pid, parse_qs(url.query), body)
        if roll < CHAOS * 2 / 3:
            return self.reply(500, {"code": "internal_server_error"})  # it happened, the caller sees an error
        if roll < CHAOS:
            time.sleep(HANG)                                           # it happened, the caller times out
        self.reply(201 if method == "POST" and not pid else 200, result)

    def reply(self, code, body, headers=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        try:
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client already gave up

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("PORT", 8081))), Handler).serve_forever()
