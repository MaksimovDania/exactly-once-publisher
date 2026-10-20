"""Platform adapters. A new platform is one class here: its `steps`, and one method per step.

Each step gets the publication (with the article's fields) and the `state` committed so far, and returns what
to add to `state`. The worker commits that before it runs the next step. Every adapter must guarantee:

  * steps before the last leave nothing visible to the site's readers (drafts, media uploads);
  * the step that creates the post first looks for one it made earlier, tagged with the article's stable uid,
    so a create that timed out but actually went through is adopted instead of duplicated;
  * the last step (publish) is idempotent and only publishes the id taken from `state`.
"""
import asyncio
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx


class Retry(Exception):
    """Transient: network error, timeout, 5xx. Retried later with backoff."""


class RateLimited(Exception):
    def __init__(self, after: float):
        super().__init__(f"rate limited for {after:.0f}s")
        self.after = after


class AuthFailed(Exception):
    """Credentials rejected: the whole site is parked until the customer reconnects it."""


class Fatal(Exception):
    """Retrying won't help (validation error, blog deleted, ...): fail the article and tell the customer."""


def retry_after(value) -> float:
    """Retry-After is either seconds or an HTTP-date (RFC 9110)."""
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return 30.0


class Adapter:
    steps: tuple[str, ...] = ()
    auth = None

    def __init__(self, site, http: httpx.AsyncClient):
        self.site, self.http = site, http
        self.gap, self.next_call = 1 / site["rate_per_sec"], 0.0

    async def call(self, method, path, ok=(), **kw):
        # The worker holds this site's lease, so pacing here is the site's rate limit: no shared limiter.
        await asyncio.sleep(self.next_call - time.monotonic())
        self.next_call = time.monotonic() + self.gap
        try:
            r = await self.http.request(method, self.site["base_url"] + path, auth=self.auth, **kw)
        except httpx.TransportError as e:  # timeouts too: the call may or may not have happened, steps cope
            raise Retry(repr(e)) from e
        if r.status_code in ok:
            return None
        if r.status_code == 429 or (r.status_code == 503 and "Retry-After" in r.headers):
            raise RateLimited(retry_after(r.headers.get("Retry-After")))
        if r.status_code in (401, 403):
            raise AuthFailed(f"{r.status_code} {r.text[:200]}")
        if r.status_code >= 500 or r.status_code == 408:
            raise Retry(f"{r.status_code} {r.text[:200]}")
        if r.status_code >= 400:
            raise Fatal(f"{r.status_code} {r.text[:200]}")
        return r.json()


class WordPress(Adapter):
    """WP REST API with an application password."""

    steps = ("media", "draft", "publish")

    def __init__(self, site, http):
        super().__init__(site, http)
        self.auth = (site["credentials"]["user"], site["credentials"]["app_password"])

    async def mine(self, pub):
        # Posts carrying our marker. Search by marker, not slug: WP renames slugs on conflict and drafts have none.
        # The marker is the article's uid, so it survives a DB restore that lost or reset the publication row.
        marker = f"<!-- ryze:{pub['article_uid']} -->"
        found = await self.call("GET", "/wp-json/wp/v2/posts", params={
            "search": str(pub["article_uid"]), "status": "draft,publish", "context": "edit"})
        return [p for p in found if marker in p["content"]["raw"]]

    async def media(self, pub, state):
        # ponytail: no lookup; a re-upload after an ambiguous timeout is an unused file in the media library.
        img = (await self.http.get(pub["image_url"])).raise_for_status().content  # our CDN, not the site
        m = await self.call("POST", "/wp-json/wp/v2/media", content=img, headers={
            # The uid, not the slug: httpx sends headers as ASCII, and slugs can be Cyrillic.
            "Content-Type": "image/jpeg", "Content-Disposition": f'attachment; filename="{pub["article_uid"]}.jpg"'})
        return {"media_id": m["id"]}

    async def draft(self, pub, state):
        # A live one first: after a DB restore the article may already be published.
        mine = sorted(await self.mine(pub), key=lambda p: (p["status"] != "publish", p["id"]))
        if mine:
            return {"post_id": mine[0]["id"]}
        # No slug on the draft: WP counts drafts when it dedupes slugs, so a leftover draft holding the slug would
        # push the live post to slug-2. The slug is set at publish time.
        p = await self.call("POST", "/wp-json/wp/v2/posts", json={
            "title": pub["title"], "status": "draft", "featured_media": state["media_id"],
            "content": pub["html"] + f"<!-- ryze:{pub['article_uid']} -->"})
        return {"post_id": p["id"]}

    async def publish(self, pub, state):
        # Leftovers of lost races or stale searches go to the trash, then publish ours. Cleanup only: they have
        # no slug, so 410 (already trashed) and 501 (site has the trash disabled) must not block publishing.
        for p in await self.mine(pub):
            if p["id"] != state["post_id"]:
                await self.call("DELETE", f"/wp-json/wp/v2/posts/{p['id']}", ok=(410, 501))
        p = await self.call("POST", f"/wp-json/wp/v2/posts/{state['post_id']}",
                            json={"status": "publish", "slug": pub["slug"]})
        # A different slug back means the customer's own post holds ours; the app shows it as a warning.
        return {"url": p["link"], "slug": p["slug"]}


ADAPTERS = {"wordpress": WordPress}
