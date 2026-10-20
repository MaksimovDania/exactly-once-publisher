"""End-to-end proof: 20 sites, a flaky platform, and publisher workers killed with SIGKILL every couple of seconds.

    docker compose up -d && python chaos.py

Passes iff every article ends up live exactly once, at its own slug. Also checks: re-running the enqueue is a
no-op, and a site with revoked credentials is parked (no attempts burned) and drains once the customer
reconnects it. The random run rarely makes two workers fight over the *same* article (the new owner of a site
takes older articles first), so `zombie_check` then forces that worst case on purpose, and `restore_check`
replays the article after a restore from an old DB backup.
"""
import asyncio
import contextlib
import json
import os
import random
import signal
import subprocess
import sys
import time
import urllib.request

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from platforms import WordPress
from publisher import LostRace, run_steps

SITES, WORKERS, ARTICLES = 20, 4, int(os.environ.get("ARTICLES", 500))
DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/publisher")
FAKE = "http://127.0.0.1:8081"
# Timers shrunk so a run takes a minute, not a day. Some sites are configured faster than the fake allows: expect 429s.
WORKER_ENV = {**os.environ, "LANES": "8", "LEASE": "5", "SLICE": "3", "HTTP_TIMEOUT": "1",
              "BACKOFF_BASE": "0.5", "BACKOFF_CAP": "5", "MAX_ATTEMPTS": "50"}
GOOD = Jsonb({"user": "ryze", "app_password": "secret"})
PUB = ("SELECT p.*, a.uid AS article_uid, a.title, a.slug, a.html, a.image_url "
       "FROM publications p JOIN articles a ON a.id = p.article_id WHERE p.id = %s")


def enqueue(db):
    # What the article pipeline does daily. (article, site) is unique, so running it again changes nothing.
    db.execute("INSERT INTO publications (article_id, destination_id) "
               "SELECT id, 1 + id %% %s FROM articles ON CONFLICT DO NOTHING", (SITES,))


@contextlib.contextmanager
def fake(port, **env):
    p = subprocess.Popen([sys.executable, "fake_platform.py"],
                         env={**os.environ, "PORT": str(port), "CHAOS": "0", "SITE_RATE": "1000", **env})
    time.sleep(0.5)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        p.kill()


async def check(base, scenario):
    site = {"base_url": f"{base}/s/1", "credentials": GOOD.obj, "rate_per_sec": 1000}
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True, row_factory=dict_row) as db, \
            httpx.AsyncClient() as http:
        async def run(pub_id, **changes):
            pub = await (await db.execute(PUB, (pub_id,))).fetchone()
            return await run_steps(db, WordPress(site, http), {**pub, "image_url": f"{base}/cdn/cover.jpg", **changes})
        pub = await scenario(db, run, http)
        live = (await http.get(f"{base}/stats")).json()["live"].get(str(pub["article_uid"]), [])
    return pub, live


async def zombie_check(base):
    """Two workers resume one publication from the same stale snapshot (a zombie and its lease's next owner),
    and search is too stale for either to see the other's draft, so both create one. Exactly one may go live."""
    async def scenario(db, run, http):
        pub = await (await db.execute(
            "UPDATE publications SET status = 'pending', step = 'draft', state = '{\"media_id\": 1}' "
            "WHERE id = (SELECT min(id::text)::uuid FROM publications) RETURNING id")).fetchone()
        results = await asyncio.gather(run(pub["id"]), run(pub["id"]), return_exceptions=True)
        scenario.outcome = sorted("lost race" if isinstance(r, LostRace) else "published" for r in results)
        return await (await db.execute(PUB, (pub["id"],))).fetchone()
    pub, live = await check(base, scenario)
    print(f"zombie check: {scenario.outcome}, live at {live}")
    assert scenario.outcome == ["lost race", "published"] and live == [pub["slug"]], "zombie: not one live post at its own slug"


async def restore_check(base):
    """Publish an article, then replay it as if the DB came back from a backup taken mid-flight (row reset to the
    draft step), and from one taken before the enqueue (row missing, enqueued again with a new id). The site has
    its trash disabled and holds a leftover draft from a lost race, which must not block publishing."""
    async def scenario(db, run, http):
        pid = (await (await db.execute("UPDATE publications SET status = 'pending', step = NULL, state = '{}' "
                                       "WHERE id = (SELECT max(id::text)::uuid FROM publications) RETURNING id")).fetchone())["id"]
        await run(pid)
        uid = (await (await db.execute(PUB, (pid,))).fetchone())["article_uid"]
        await http.post(f"{base}/s/1/wp-json/wp/v2/posts", auth=("ryze", "secret"),
                        json={"title": "leftover", "status": "draft", "content": f"<!-- ryze:{uid} -->"})
        await db.execute("UPDATE publications SET status = 'pending', step = 'draft', state = state - 'post_id' "
                         "WHERE id = %s", (pid,))
        await run(pid)
        row = await (await db.execute("DELETE FROM publications WHERE id = %s RETURNING article_id, destination_id",
                                      (pid,))).fetchone()
        pid = (await (await db.execute("INSERT INTO publications (article_id, destination_id) VALUES (%s, %s) RETURNING id",
                                       (row["article_id"], row["destination_id"]))).fetchone())["id"]
        await run(pid)
        return await (await db.execute(PUB, (pid,))).fetchone()
    pub, live = await check(base, scenario)
    print(f"restore check (trash disabled): published, replayed from step draft, re-enqueued: live at {live}")
    assert live == [pub["slug"]], "restore: not one live post at its own slug"


def counts(db):
    return dict(db.execute("SELECT status, count(*) FROM publications GROUP BY status").fetchall())


def main():
    db = psycopg.connect(DSN, autocommit=True)
    db.execute("DROP TABLE IF EXISTS publications, articles, destinations")
    db.execute(open("schema.sql").read())
    for i in range(SITES):  # site 1 starts with a revoked password
        db.execute("INSERT INTO destinations (platform, base_url, credentials, rate_per_sec) VALUES ('wordpress', %s, %s, %s)",
                   (f"{FAKE}/s/{i + 1}", Jsonb({"user": "ryze", "app_password": "revoked"}) if i == 0 else GOOD,
                    random.choice([5, 8, 20])))
    db.execute("INSERT INTO articles (title, slug, html, image_url) "
               "SELECT 'Article ' || g, CASE WHEN g %% 10 = 0 THEN 'статья-' ELSE 'article-' END || g, '<p>Hello</p>', %s "
               "FROM generate_series(1, %s) g",  # every 10th slug is Cyrillic
               (f"{FAKE}/cdn/cover.jpg", ARTICLES))
    enqueue(db)
    enqueue(db)

    fake_proc = subprocess.Popen([sys.executable, "fake_platform.py"])
    logfile = open("workers.log", "w")
    spawn = lambda: subprocess.Popen([sys.executable, "publisher.py"], env=WORKER_ENV, stdout=logfile, stderr=logfile)
    workers = [spawn() for _ in range(WORKERS)]
    kills, freezes, frozen, start, parked = 0, 0, None, time.time(), None
    try:
        while (c := counts(db)).get("pending") and time.time() - start < 600:
            time.sleep(random.uniform(1, 3))
            if frozen and time.time() - frozen[1] > 6:  # wakes up after its leases expired: a zombie
                frozen[0].send_signal(signal.SIGCONT)
                frozen = None
            victim = random.choice([w for w in workers if not frozen or w is not frozen[0]])
            if not frozen and random.random() < 0.3:  # GC pause / frozen VM, longer than LEASE
                victim.send_signal(signal.SIGSTOP)
                frozen, freezes = (victim, time.time()), freezes + 1
            else:
                victim.kill()  # SIGKILL: no cleanup, no goodbye
                victim.wait()
                workers[workers.index(victim)] = spawn()
                kills += 1
            print(f"{time.time() - start:5.0f}s  {c}  kills={kills} freezes={freezes}")
            if not parked and time.time() - start > 15:
                parked = db.execute("SELECT count(*), max(attempts) FROM publications "
                                    "WHERE destination_id = 1 AND status = 'pending'").fetchone()
                db.execute("UPDATE destinations SET credentials = %s, status = 'active' WHERE id = 1", (GOOD,))
                print(f"site 1 reconnected; it had {parked[0]} articles parked, max attempts {parked[1]}")
    finally:
        for w in workers:
            w.send_signal(signal.SIGCONT)
            w.terminate()  # graceful: finish the current article, release the site
        for w in workers:
            w.wait()
        stats = json.load(urllib.request.urlopen(f"{FAKE}/stats"))
        fake_proc.kill()

    rows = db.execute("SELECT a.uid::text, a.slug FROM publications p JOIN articles a ON a.id = p.article_id").fetchall()
    twice = [u for u, _ in rows if len(stats["live"].get(u, [])) > 1]
    never = [u for u, _ in rows if not stats["live"].get(u)]
    moved = [(slug, stats["live"][u]) for u, slug in rows if len(stats["live"].get(u, [])) == 1 and stats["live"][u] != [slug]]
    retries = db.execute("SELECT sum(attempts) - count(*) FROM publications").fetchone()[0]
    with open("workers.log") as f:
        log = f.read()
    print(f"\n{len(rows)} publications for {ARTICLES} articles (enqueued twice), {counts(db)}")
    print(f"workers SIGKILLed: {kills}, frozen past their lease: {freezes}, retries: {retries}, 429s: {log.count('rate limited')}, "
          f"lost races: {log.count('lost race')}, leftover drafts trashed: {stats['trashed']}, still drafts: {sum(stats['drafts'].values())}")
    print(f"live twice: {len(twice)}, never live: {len(never)}, live at the wrong slug: {len(moved)} {moved[:3]}")
    assert len(rows) == ARTICLES and not twice and not never, "NOT exactly once"
    assert not moved, "a leftover draft pushed the live post off its slug"
    assert parked and parked[0] > 0 and parked[1] == 0, f"parked site burned attempts: {parked}"
    assert "ValueError" not in log, "unparsed Retry-After"
    with fake(8082, SEARCH_LAG="60") as base:
        asyncio.run(zombie_check(base))
    with fake(8083, SEARCH_LAG="0", NO_TRASH="1") as base:
        asyncio.run(restore_check(base))
    print("OK: every article is live exactly once, at its own slug")


if __name__ == "__main__":
    main()
