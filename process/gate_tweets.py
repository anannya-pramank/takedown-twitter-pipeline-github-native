"""
Relevance gate — a plain step in the twitter-pipeline workflow, run right
after fetch_tweets.py in the same job. No queue: at this volume (a few
hundred tweets per run), a queue + separate Lambda existed only to decouple
retry semantics from the fetch task, which a single sequential job doesn't
need — a per-item try/except gets you the same isolation.

Two-tier gate, cheapest-first:
  Tier 1 (free, always runs): embed the tweet with all-MiniLM-L6-v2 — same
    model your other pipelines already use — and take cosine similarity
    against a small set of reference sentences describing what a real
    takedown-related tweet looks like.
      score >= KEEP_THRESHOLD  -> auto_keep, no LLM call
      score <  DROP_THRESHOLD  -> auto_drop, no LLM call, not written to DB
      in between                -> ambiguous, escalate to tier 2
  Tier 2 (only the ambiguous band): one short Gemini Flash call, free-tier
    eligible, asking a single yes/no relevance question. A transient Gemini
    failure keeps the tweet for human review rather than dropping it.

Dedup: sha1(tweet_id) as primary key. Insert uses `on conflict (tweet_id) do
nothing` directly — no pre-check SELECT — and reads cur.rowcount to tell
"kept" from "skipped", which halves DB round-trips versus the Lambda version.

Failure handling: one bad record (malformed body, transient DB error) is
logged and appended to a local failed-items.json rather than raising — that
file gets uploaded as a workflow artifact, giving you the DLQ's inspect-and-
replay value without an actual queue.

Env vars expected (set by the workflow):
  DATABASE_URL      - Supabase Postgres connection string (postgresql://...)
  GEMINI_API_KEY    - optional; missing key defaults ambiguous tweets to keep
  RAW_INPUT_PATH    - the raw JSON file fetch_tweets.py just wrote
  FAILED_OUTPUT_PATH - where to write failed items for the artifact upload
"""
import hashlib
import json
import os

import psycopg2
import requests
from sentence_transformers import SentenceTransformer, util

KEEP_THRESHOLD = 0.55
DROP_THRESHOLD = 0.30

REFERENCE_SENTENCES = [
    "The Indian government ordered a website or app blocked under Section 69A of the IT Act.",
    "A court in India issued an order to take down or block online content.",
    "MeitY published a compliance report or blocking order affecting online platforms.",
    "A social media account was withheld or restricted specifically in India following a legal request.",
    "Content was removed from a platform in India under the IT Rules 2021.",
]


def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        # Supabase enforces TLS; fail loudly instead of silently connecting
        # in plaintext if that ever changes.
        sslmode="require",
        connect_timeout=10,
    )


def sha1_id(tweet_id):
    return hashlib.sha1(tweet_id.encode("utf-8")).hexdigest()


def to_vector_literal(embedding):
    """pgvector's text input form is '[f1,f2,...]'. We insert with an explicit
    ::vector cast (see insert_row) so the parameter type is never ambiguous —
    a plain Python list handed to psycopg2 would be adapted as a Postgres
    array and rejected by the vector column."""
    return "[" + ",".join(f"{x:.7f}" for x in embedding) + "]"


def tier1_score(text, model, reference_embeddings):
    embedding = model.encode(text, convert_to_tensor=True)
    sims = util.cos_sim(embedding, reference_embeddings)
    return float(sims.max()), embedding.cpu().numpy().tolist()


def tier2_gemini_check(text, api_key):
    """Single short call — asks for one word back to keep token usage minimal.
    Raises on transport/HTTP/parse errors; the caller decides what to do with
    a failure (here: keep for human review)."""
    prompt = (
        "Reply with exactly one word, YES or NO. "
        "Is this tweet about an Indian government or court content takedown, "
        "website/app block, or account restriction order?\n\n"
        f"Tweet: {text[:500]}"
    )
    resp = requests.post(
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={api_key}",
        json={"contents": [{"parts": [{"text": prompt}]}]},
        timeout=10,
    )
    resp.raise_for_status()
    out = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip().upper()
    return out.startswith("YES"), out


def insert_row(cur, row):
    """Returns True if a new row was actually inserted, False if the
    tweet_id already existed (on conflict do nothing)."""
    cur.execute(
        """
        insert into takedown_tweets
            (id, tweet_id, author_handle, text, url, posted_at,
             source_type, source_value, run_id,
             relevance_score, relevance_tier, llm_verdict, embedding)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
        on conflict (tweet_id) do nothing
        """,
        (
            row["id"], row["tweet_id"], row.get("author_handle"), row["text"],
            row.get("url"), row.get("posted_at"),
            row["source_type"], row["source_value"], row["run_id"],
            row["relevance_score"], row["relevance_tier"], row.get("llm_verdict"),
            row["embedding"],   # already a '[...]' literal, cast to vector in SQL
        ),
    )
    return cur.rowcount == 1


def process_item(item, model, reference_embeddings, gemini_api_key, cur):
    """Returns one of: 'kept' | 'dropped' | 'skipped' | None (empty item).
    Raises on anything the caller should log to failed-items.json."""
    tweet_id = str(item.get("id") or item.get("tweet_id") or "")
    text = item.get("text", "")
    if not tweet_id or not text:
        return None

    score, embedding = tier1_score(text, model, reference_embeddings)

    if score >= KEEP_THRESHOLD:
        tier, llm_verdict = "auto_keep", None
    elif score < DROP_THRESHOLD:
        return "dropped"  # never written — keeps the table free of obvious noise
    else:
        if not gemini_api_key:
            # no key configured -> keep for human review rather than silently drop
            tier, llm_verdict = "llm_reviewed", "NO_API_KEY_DEFAULTED_KEEP"
        else:
            try:
                is_relevant, raw_verdict = tier2_gemini_check(text, gemini_api_key)
            except Exception as e:
                # Transient Gemini failure (rate limit, timeout, 5xx, bad JSON):
                # keep for human review rather than dropping on a fluke.
                tier = "llm_reviewed"
                llm_verdict = f"GEMINI_ERROR_DEFAULTED_KEEP:{type(e).__name__}"
            else:
                tier, llm_verdict = "llm_reviewed", raw_verdict
                if not is_relevant:
                    return "dropped"

    row = {
        "id": sha1_id(tweet_id),
        "tweet_id": tweet_id,
        "author_handle": item.get("author") or item.get("username"),
        "text": text,
        "url": item.get("url"),
        "posted_at": item.get("created_at") or item.get("posted_at"),
        "source_type": item["_source_type"],
        "source_value": item["_source_value"],
        "run_id": item["_run_id"],
        "relevance_score": score,
        "relevance_tier": tier,
        "llm_verdict": llm_verdict,
        "embedding": to_vector_literal(embedding),
    }
    inserted = insert_row(cur, row)
    return "kept" if inserted else "skipped"


def write_step_summary(kept, dropped, skipped, failed):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a") as f:
        f.write("### Relevance gate\n\n")
        f.write(f"- kept: **{kept}**\n- dropped: **{dropped}**\n"
                f"- skipped (dupe): **{skipped}**\n- failed: **{len(failed)}**\n")


def main():
    raw_path = os.environ.get("RAW_INPUT_PATH")
    with open(raw_path) as f:
        items = json.load(f)

    model = SentenceTransformer("all-MiniLM-L6-v2")
    reference_embeddings = model.encode(REFERENCE_SENTENCES, convert_to_tensor=True)
    gemini_api_key = os.environ.get("GEMINI_API_KEY")

    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()

    kept, dropped, skipped = 0, 0, 0
    failed = []

    for item in items:
        try:
            outcome = process_item(item, model, reference_embeddings, gemini_api_key, cur)
            if outcome == "kept":
                kept += 1
            elif outcome == "dropped":
                dropped += 1
            elif outcome == "skipped":
                skipped += 1
        except Exception as e:
            tweet_id = item.get("id") or item.get("tweet_id")
            print(f"[error] tweet_id={tweet_id}: {type(e).__name__}: {e}")
            failed.append({"tweet_id": tweet_id, "error": f"{type(e).__name__}: {e}", "item": item})

    cur.close()
    conn.close()

    print(f"[info] kept={kept} dropped={dropped} skipped_dupe={skipped} failed={len(failed)}")
    write_step_summary(kept, dropped, skipped, failed)

    failed_path = os.environ.get("FAILED_OUTPUT_PATH", "failed-items.json")
    if failed:
        with open(failed_path, "w") as f:
            json.dump(failed, f, ensure_ascii=False)
        print(f"[warn] wrote {len(failed)} failed items to {failed_path} for the workflow artifact")


if __name__ == "__main__":
    main()
