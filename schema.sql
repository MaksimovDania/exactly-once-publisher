-- A customer site we publish to. The site is the unit of concurrency: its rate limit, its auth token and
-- its outages all live here, so exactly one worker talks to a given site at a time (see publisher.py).
CREATE TABLE IF NOT EXISTS destinations (
    id           bigserial   PRIMARY KEY,
    platform     text        NOT NULL,                     -- 'wordpress' | 'shopify' | 'webflow' | 'wix' | ...
    base_url     text        NOT NULL,
    credentials  jsonb       NOT NULL DEFAULT '{}',        -- ponytail: plaintext; prod = KMS-encrypted column
    rate_per_sec real        NOT NULL DEFAULT 2,
    status       text        NOT NULL DEFAULT 'active',    -- 'active' | 'needs_reauth' | 'disabled'
    lease_until  timestamptz NOT NULL DEFAULT 'epoch'      -- leased by a worker until then; also "back off until" after a 429
);

-- Written by the article-generation pipeline; the publisher only reads it.
CREATE TABLE IF NOT EXISTS articles (
    id        bigserial PRIMARY KEY,
    uid       uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),  -- stable identity, stamped on the remote post as our marker
    title     text NOT NULL,
    slug      text NOT NULL,
    html      text NOT NULL,
    image_url text NOT NULL
);

-- One article going to one site. This row is the job, the state machine and the audit trail at once:
-- no separate queue, so there is nothing to keep in sync with it.
CREATE TABLE IF NOT EXISTS publications (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id     bigint      NOT NULL REFERENCES articles,
    destination_id bigint      NOT NULL REFERENCES destinations,
    status         text        NOT NULL DEFAULT 'pending',             -- 'pending' | 'done' | 'failed'
    step           text,                                               -- next step to run (NULL = first); the adapter defines the sequence
    state          jsonb       NOT NULL DEFAULT '{}',                  -- committed step outputs: media id, remote post id, url
    attempts       int         NOT NULL DEFAULT 0,
    run_at         timestamptz NOT NULL DEFAULT now(),                 -- scheduled publish time, later the next retry time
    last_error     text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (article_id, destination_id)                                -- re-running the daily enqueue can't create a second job
);

CREATE INDEX IF NOT EXISTS publications_due ON publications (destination_id, run_at) WHERE status = 'pending';
