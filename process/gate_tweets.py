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

KEEP_THRESHOLD = 0.62
DROP_THRESHOLD = 0.35

REFERENCE_SENTENCES = [
    "The Indian government ordered a website or app blocked under Section 69A of the IT Act.",
    "A court in India issued an order to take down or block online content.",
    "MeitY published a compliance report or blocking order affecting online platforms.",
    "A social media account was withheld or restricted specifically in India following a legal request.",
    "Content was removed from a platform in India under the IT Rules 2021.",
]


def get_db_connection():
    """Connect with discrete parameters rather than a single DSN string.
    Supabase-generated passwords routinely contain characters like `%`, `@`,
    or `/` that are only safe inside a postgresql:// URI if percent-encoded
    — a single wrong character breaks the DSN parser. Passing password as
    its own keyword argument sidesteps that entirely; psycopg2 takes it as
    a plain string with no URI parsing involved.

    Uses `or` rather than os.environ.get(key, default): an unset GitHub
    Actions secret still creates the env var, just as an empty string, so
    the key is always "present" and dict.get's default never actually
    fires. `or` falls back on any falsy value, empty string included."""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT") or "5432",
        dbname=os.environ.get("SUPABASE_DB_NAME") or "postgres",
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"],
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
    a failure (here: keep for human review).

    Exclusions below are anchored to real false positives observed in
    production, not hypothetical ones: "Section 69A" collides with the
    unrelated Income Tax Act provision constantly; generic e-commerce
    complaints ("Grievance Officer, I am writing to lodge a complaint
    regarding Order #...") share vocabulary with the IT Rules' Grievance
    Officer mechanism; org accounts tracked via handles (IFF, SFLC, PUCL)
    post heavily about adjacent-but-different topics (privacy law, arrests,
    unrelated litigation) that share keywords without describing an actual
    block/takedown."""
    prompt = (
        "Reply with exactly one word, YES or NO.\n\n"
        "Does this tweet directly report, describe, or discuss a SPECIFIC "
        "content-blocking, website/app-blocking, account-withholding, or "
        "takedown action taken by an Indian government body, court, or "
        "platform acting under Indian law (Section 69A/79 IT Act, IT Rules "
        "2021, MeitY, the Sahyog portal, the Grievance Appellate Committee, "
        "or a named blocking/takedown order)? Reporting on or analyzing a "
        "real blocking/takedown trend (e.g. aggregate statistics on order "
        "volume) counts as YES even without naming one specific instance.\n\n"
        "Answer NO if the tweet:\n"
        "- uses a similarly-worded but unrelated law (Income Tax Act "
        "Section 69A on unexplained money, GST Section 69, or any non-IT-Act "
        "statute)\n"
        "- is a generic e-commerce or customer-service complaint, even if "
        "it uses the phrase \"grievance officer\" or \"order\"\n"
        "- is about a country other than India\n"
        "- discusses privacy law, digital rights, arrests, or litigation "
        "generally without describing an actual content block or takedown\n\n"
        f"Tweet: {text[:600]}"
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

    author = item.get("author")
    if isinstance(author, dict):
        # twitter-cli's actual shape: {"id", "name", "screenName", ...} —
        # not a plain string, so pull the handle out rather than handing
        # the whole dict to psycopg2 (which can't adapt it to a text column).
        author_handle = author.get("screenName") or author.get("name")
    else:
        author_handle = author or item.get("username")

    # X's permalink structure is predictable, so build one rather than
    # leaving url NULL — there's no top-level "url" field in this shape;
    # "urls" is a list of links *mentioned in* the tweet, not the tweet's
    # own permalink.
    url = (f"https://x.com/{author_handle}/status/{tweet_id}"
           if author_handle else None)

    row = {
        "id": sha1_id(tweet_id),
        "tweet_id": tweet_id,
        "author_handle": author_handle,
        "text": text,
        "url": url,
        # twitter-cli returns createdAt/createdAtISO (camelCase), not
        # created_at/posted_at — this was silently NULL on every row before.
        "posted_at": item.get("createdAtISO") or item.get("createdAt"),
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
