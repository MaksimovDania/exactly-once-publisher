"""Publisher worker. Run as many copies as you like, anywhere, and kill them whenever:

    python publisher.py

Loop: lease one customer site that has due articles -> publish them one at a time, committing to Postgres after
every remote call -> release the site so the next one gets a turn.

Why every article ends up live exactly once, wherever we crash (platforms.py holds the adapter half):
  1. One publications row per (article, site), so re-running the daily enqueue is a no-op.
  2. Each step's result is committed with compare-and-set on `step`, so exactly one attempt's result wins
     a step. A worker that loses (say, a zombie whose lease expired) stops.
  3. Nothing is visible until the last step, which is idempotent and only ever applied to the post id
     that won step 2.
Leases and timeouts only decide who works when. Correctness never depends on them.
"""
import asyncio
import logging
import os
import random
import signal
import time

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from platforms import ADAPTERS, AuthFailed, Fatal, RateLimited

env = os.environ.get
DSN = env("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/publisher")
LANES = int(env("LANES", 20))         # sites this process serves concurrently
LEASE = float(env("LEASE", 60))       # seconds a site stays ours; after a crash it frees itself
SLICE = float(env("SLICE", 20))       # then the site goes to the back of the line (fairness between customers)
MAX_ATTEMPTS = int(env("MAX_ATTEMPTS", 10))
BACKOFF_BASE, BACKOFF_CAP = float(env("BACKOFF_BASE", 30)), float(env("BACKOFF_CAP", 3600))
HTTP_TIMEOUT = float(env("HTTP_TIMEOUT", 15))

log = logging.getLogger("publisher")

# Least recently served active site with something due. SKIP LOCKED: workers never wait on each other.
# ponytail: the EXISTS probe walks idle sites; past ~100k sites keep a next_due_at column on destinations.
LEASE_SITE = """
UPDATE destinations SET lease_until = now() + make_interval(secs => %(lease)s)
WHERE id = (SELECT d.id FROM destinations d
            WHERE d.status = 'active' AND d.lease_until < now()
              AND EXISTS (SELECT 1 FROM publications p
                          WHERE p.destination_id = d.id AND p.status = 'pending' AND p.run_at <= now())
            ORDER BY d.lease_until LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING *"""

# Take the site's next due article and push its run_at out by the backoff *before* working on it: if we die
# halfway it comes back on its own, and a crash loop burns attempts like any other failure.
PICK = """
UPDATE publications p
SET attempts = p.attempts + 1,
    run_at = now() + make_interval(secs => least(%(cap)s, %(base)s * 2 ^ p.attempts) * (0.5 + random()))
FROM articles a
WHERE a.id = p.article_id AND p.id = (
    SELECT id FROM publications
    WHERE destination_id = %(site)s AND status = 'pending' AND run_at <= now()
    ORDER BY run_at LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING p.*, a.uid AS article_uid, a.title, a.slug, a.html, a.image_url"""

CHECKPOINT = """
UPDATE publications SET step = %(next)s, state = %(state)s, status = %(status)s, last_error = NULL, updated_at = now()
WHERE id = %(id)s AND status = 'pending' AND step IS NOT DISTINCT FROM %(step)s"""

# Not the article's fault (site throttled or disconnected): give the attempt back.
REFUND = "UPDATE publications SET attempts = attempts - 1, run_at = now(), last_error = %s WHERE id = %s"


class LostRace(Exception):
    """Another worker committed this step first. Its result counts; ours is an invisible leftover."""


async def run_steps(db, adapter, pub):
    steps, state, step = adapter.steps, pub["state"], pub["step"]
    for i in range(steps.index(step) if step else 0, len(steps)):
        state |= await getattr(adapter, steps[i])(pub, state)
        last = i == len(steps) - 1
        nxt = "done" if last else steps[i + 1]
        cur = await db.execute(CHECKPOINT, {"id": pub["id"], "step": step, "next": nxt, "state": Jsonb(state),
                                            "status": "done" if last else "pending"})
        if cur.rowcount != 1:
            raise LostRace
        step = nxt
    return state


async def serve(db, http, site, stopping):
    adapter = ADAPTERS[site["platform"]](site, http)
    pause, until = 0.0, time.monotonic() + SLICE
    while time.monotonic() < until and not stopping.is_set():
        pub = await (await db.execute(PICK, {"site": site["id"], "base": BACKOFF_BASE, "cap": BACKOFF_CAP})).fetchone()
        if not pub:
            break
        try:
            if pub["attempts"] > MAX_ATTEMPTS:
                raise Fatal(f"gave up after {MAX_ATTEMPTS} attempts, last error: {pub['last_error']}")
            state = await run_steps(db, adapter, pub)
            log.info("published %s -> %s (attempt %d)", pub["id"], state["url"], pub["attempts"])
            if state["slug"] != pub["slug"]:
                log.warning("slug %s was taken on site %s, live as %s", pub["slug"], site["id"], state["slug"])
        except RateLimited as e:  # the whole site backs off, not just this article
            await db.execute(REFUND, (str(e), pub["id"]))
            log.warning("site %s %s", site["id"], e)
            pause = e.after
            break
        except AuthFailed as e:  # prod: refresh the OAuth token first (we own the site, so no refresh races)
            await db.execute(REFUND, (str(e), pub["id"]))
            # Only if these are still the credentials that failed: the customer may have reconnected meanwhile.
            await db.execute("UPDATE destinations SET status = 'needs_reauth' WHERE id = %s AND credentials = %s",
                             (site["id"], Jsonb(site["credentials"])))
            log.warning("site %s needs reauth: %s", site["id"], e)
            break
        except Fatal as e:
            await db.execute("UPDATE publications SET status = 'failed', last_error = %s WHERE id = %s AND status = 'pending'",
                             (str(e), pub["id"]))
            log.error("failed %s: %s", pub["id"], e)
        except LostRace:
            log.warning("lost race on %s, another worker owns it", pub["id"])
        except Exception as e:  # transient or unknown: PICK already scheduled the retry with backoff
            await db.execute("UPDATE publications SET last_error = %s WHERE id = %s", (repr(e)[:500], pub["id"]))
            log.warning("retry %s later (attempt %d): %r", pub["id"], pub["attempts"], e)
    # Only if the lease is still ours: a zombie must not cut short the next owner's lease.
    await db.execute("UPDATE destinations SET lease_until = now() + make_interval(secs => %s) WHERE id = %s AND lease_until = %s",
                     (pause, site["id"], site["lease_until"]))


async def lane(http, stopping):
    while not stopping.is_set():
        try:
            async with await psycopg.AsyncConnection.connect(DSN, autocommit=True, row_factory=dict_row) as db:
                while not stopping.is_set():
                    site = await (await db.execute(LEASE_SITE, {"lease": LEASE})).fetchone()
                    if site:
                        await serve(db, http, site, stopping)
                    else:
                        await asyncio.sleep(0.5 + random.random())
        except Exception:  # DB restart/failover: whatever was committed stands, the rest is retried
            log.exception("lane crashed, reconnecting")
            await asyncio.sleep(1)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s pid=%(process)d %(levelname)s %(message)s")
    stopping = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):  # deploys: finish the current article, release sites, exit
        asyncio.get_running_loop().add_signal_handler(sig, stopping.set)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as http:
        await asyncio.gather(*(lane(http, stopping) for _ in range(LANES)))


if __name__ == "__main__":
    asyncio.run(main())
