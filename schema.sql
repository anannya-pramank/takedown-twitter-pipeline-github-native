-- Run once against your Supabase project this pipeline writes to (SQL editor,
-- or psql against SUPABASE_DB_URL). Mirrors the CERC pipeline's conventions:
-- sha1 doc id, embedding column, HNSW cosine index.
--
-- Enable the `vector` extension first via Database -> Extensions in the
-- Supabase dashboard (or `create extension` below works too if your role
-- has the privilege — Supabase projects normally do).

create extension if not exists vector;

create table if not exists takedown_tweets (
    id              text primary key,            -- sha1(tweet_id)
    tweet_id        text not null unique,
    author_handle   text,
    text            text not null,
    url             text,
    posted_at       timestamptz,
    source_type     text not null,                -- 'query' | 'handle'
    source_value    text not null,                 -- the query string or handle
    run_id          text not null,
    relevance_score real,                          -- tier-1 cosine similarity
    relevance_tier  text not null,                  -- 'auto_keep' | 'auto_drop' | 'llm_reviewed'
    llm_verdict     text,                            -- only set when tier-2 ran
    embedding       vector(384),                      -- all-MiniLM-L6-v2 dim
    discovered_at   timestamptz not null default now(),
    status          text not null default 'new'        -- 'new' | 'reviewed' | 'linked_to_order'
);

create index if not exists takedown_tweets_embedding_idx
    on takedown_tweets using hnsw (embedding vector_cosine_ops);

create index if not exists takedown_tweets_status_idx on takedown_tweets (status);
create index if not exists takedown_tweets_run_idx on takedown_tweets (run_id);

-- Quick coverage view, same spirit as CERC's v_tagging_progress
create or replace view v_takedown_tweet_coverage as
select
    source_type,
    source_value,
    count(*) as total,
    count(*) filter (where relevance_tier = 'auto_keep') as auto_kept,
    count(*) filter (where relevance_tier = 'llm_reviewed') as llm_reviewed,
    max(discovered_at) as last_seen
from takedown_tweets
group by source_type, source_value
order by last_seen desc;
