"""
Fetch step — runs as a step in the twitter-pipeline GitHub Actions workflow.

Flow per run:
  1. Read Twitter cookies + proxy URL straight from the environment (GitHub
     Actions injects repo secrets as env vars — no Secrets Manager call).
  2. For each configured query/handle, call twitter-cli as a subprocess,
     following Agent Reach's own retry chain (retry once, then fall back
     to a more stable command) rather than failing the whole run on a
     single 404.
  3. Write raw results as one JSON file for this run to a local path (the
     next workflow step commits it to the raw-data branch — nothing here
     is ever the only copy of the data).
  4. Print a `kept=/dropped=` line and a $GITHUB_STEP_SUMMARY block so the
     Actions UI shows run health without opening logs.

  Never call `twitter status` — Agent Reach's own docs warn this can trigger
  unwanted browser-cookie fallback behavior; success/failure of the real
  calls IS the health signal, same as before.

Env vars expected (set by the workflow):
  TWITTER_AUTH_TOKEN, TWITTER_CT0   - from repo secrets
  PROXY_URL                         - optional, from repo secrets
  CONFIG_PATH                       - path to keywords.yaml (default: config/keywords.yaml)
  RAW_OUTPUT_PATH                   - where to write this run's raw JSON (default: raw/<run_id>.json)
"""
import json
import os
import subprocess
import time
from datetime import datetime, timezone

import yaml

# Prefer the workflow's own RUN_ID (set once in a shared step via
# $GITHUB_ENV) so the raw filename, the git commit message, and each row's
# run_id column in Postgres all agree — falls back to generating one if run
# standalone (e.g. local testing).
RUN_ID = os.environ.get("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_child_env():
    """Explicit credentials only — never rely on twitter-cli's browser-cookie
    auto-fallback, per Agent Reach's own security guidance. The proxy vars
    are set on the CHILD env only, matching the original isolation intent
    (nothing else in this step needs the proxy)."""
    env = os.environ.copy()
    env["TWITTER_AUTH_TOKEN"] = os.environ["TWITTER_AUTH_TOKEN"]
    env["TWITTER_CT0"] = os.environ["TWITTER_CT0"]
    proxy_url = os.environ.get("PROXY_URL")
    if proxy_url:
        env["HTTP_PROXY"] = proxy_url
        env["HTTPS_PROXY"] = proxy_url
    return env


def run_twitter_cli(args, env, timeout=30):
    """Runs a twitter-cli command, returns parsed JSON or None on failure.
    Never raises — a single failed call must not kill the whole run."""
    cmd = ["twitter"] + args + ["--json"]
    try:
        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            print(f"[warn] {' '.join(cmd)} failed: {result.stderr[:300]}")
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        print(f"[warn] {' '.join(cmd)} error: {e}")
        return None


def search_with_retry(query, n, env):
    """Mirrors Agent Reach's own documented retry chain for `twitter search`:
    retry once, then give up on search for this query (search is the
    documented-unstable command; we don't chase it further than one retry
    inside an unattended run — a human can dig deeper manually if a query
    consistently fails)."""
    for attempt in range(2):
        data = run_twitter_cli(["search", query, "-n", str(n)], env)
        if data is not None:
            return data
        time.sleep(3)
    print(f"[warn] search exhausted retries for query: {query}")
    return []


def fetch_handle_timeline(handle, n, env):
    data = run_twitter_cli(["user-posts", f"@{handle}", "-n", str(n)], env)
    return data or []


def normalize(items, source_type, source_value):
    """Tag each raw item with where it came from so downstream processing
    doesn't need to guess."""
    out = []
    for item in items:
        item["_source_type"] = source_type   # "query" | "handle"
        item["_source_value"] = source_value
        item["_run_id"] = RUN_ID
        out.append(item)
    return out


def write_step_summary(all_items, any_success):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a") as f:
        f.write(f"### Fetch — run `{RUN_ID}`\n\n")
        f.write(f"- items fetched: **{len(all_items)}**\n")
        f.write(f"- any_success: **{any_success}**\n")


def main():
    env = build_child_env()

    with open(os.environ.get("CONFIG_PATH", "config/keywords.yaml")) as f:
        config = yaml.safe_load(f)

    all_items = []
    any_success = False

    for query in config.get("queries", []):
        items = search_with_retry(query, config.get("results_per_query", 15), env)
        if items:
            any_success = True
        all_items.extend(normalize(items, "query", query))
        time.sleep(2.5)  # per-call spacing — avoid tripping rate limits

    for handle in config.get("handles", []):
        items = fetch_handle_timeline(handle, config.get("results_per_handle", 20), env)
        if items:
            any_success = True
        all_items.extend(normalize(items, "handle", handle))
        time.sleep(2.5)

    # Raw landing — always written, even if empty, so a silent zero-result
    # run is visible in the data itself, not just inferred. The workflow's
    # next step commits this file to the raw-data branch.
    out_path = os.environ.get("RAW_OUTPUT_PATH", f"raw/{RUN_ID}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_items, f, ensure_ascii=False)
    print(f"[info] wrote {len(all_items)} raw items to {out_path}")

    write_step_summary(all_items, any_success)

    if not any_success:
        # Non-zero exit fails the workflow run — that's the health signal
        # GitHub itself watches (failure email + the auto-filed issue step).
        raise SystemExit("no query or handle returned data this run — check cookie validity")


if __name__ == "__main__":
    main()
