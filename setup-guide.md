# Setup guide — GitHub-native

No AWS account, no CloudShell, no Docker builds. Five steps.

## 1. Schema

In your Supabase project: enable the `vector` extension (Database ->
Extensions), then run `infra/schema.sql` once — via the SQL editor, or
`psql "$SUPABASE_DB_URL" -f infra/schema.sql` if you'd rather do it locally.
Same table and conventions as the CERC pipeline, so if you're reusing that
project, this is just one more table in it.

## 2. Repo secrets

Settings -> Secrets and variables -> Actions -> New repository secret:

| Secret | Value |
|---|---|
| `TWITTER_AUTH_TOKEN` | from your Cookie-Editor export |
| `TWITTER_CT0` | from your Cookie-Editor export |
| `PROXY_URL` | `http://<user>:<pass>@<host>:<port>` (Webshare) |
| `SUPABASE_DB_URL` | `postgresql://...` connection string from Supabase project settings |
| `GEMINI_API_KEY` | free tier is fine |

## 3. Confirm twitter-cli

Same caveat the old Dockerfile carried: confirm the package name and
subcommands (`search`, `user-posts`, `--json`) match your Agent Reach build
before trusting the schedule. `.github/workflows/twitter-pipeline.yml`
installs it fresh on every run via `pipx install twitter-cli`.

## 4. First run

Push, then trigger `twitter-pipeline` manually from the Actions tab
(`workflow_dispatch`) rather than waiting for the schedule. Check:

- the job summary (Actions -> this run) for the fetch/gate counts,
- the `raw-data` branch for `raw/<run_id>.json`,
- `select count(*) from takedown_tweets;` in Supabase,
- the run's logs for anything twitter-cli complained about,
- no `failed-items-*` artifact attached to the run (if one appears, that's
  your DLQ equivalent — open it to see what didn't make it into the table).

## 5. Ongoing

The workflow runs every 6 hours (`schedule: cron("0 */6 * * *")`) and on
demand. A failed run fails the job outright (fetch raises `SystemExit` on
zero results, same as before), which GitHub emails to repo watchers by
default — and the workflow also files or updates a "Twitter pipeline run
failed" issue so it's visible even with email notifications off, and closes
it automatically on the next successful run.

Note on scheduling: GitHub can delay `schedule`-triggered runs during
platform load, more so than EventBridge's dedicated scheduler. For a
monitoring feed running every 6 hours this is very unlikely to matter, but
if a run is ever more than an hour or two late, that's the first thing to
check before assuming the pipeline itself is broken.
