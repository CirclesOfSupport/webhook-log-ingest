# webhook-log-ingest

Hourly ingest of the TextIt webhook HTTP log into BigQuery, plus an independent
freshness check. Two Cloud Run **jobs**, one image, one repo.

I built this to replace a Windows Scheduled Task that ran on my workstation. It is
the source for `RESPONSES.webhook_log` and `RESPONSES.webhook_log_detail`, which
back the `webhook-log-api` service.

---

## Why this exists

TextIt exposes **no API** for the webhook HTTP log. The workspace advertises 24
endpoints; `http_logs` and `webhook_events` both 404. The only access is the
authenticated HTML console at `textit.com/httplog/webhooks/`, so this pages that
console and parses it.

And TextIt retains only **~3.85 days** of that log. Anything not ingested inside
that window is deleted at the source and is **unrecoverable from anywhere**. That
retention floor is the deadline behind every design decision in this repo — it is
why the job fails loudly rather than writing zero rows, and why silent failure is
the primary risk here rather than a bug.

## Why it moved off my machine

The local version ran hourly on an always-on workstation and authenticated to
BigQuery with **user ADC**. Two things broke, and the first was not hypothetical:

1. **User ADC expired on the first scheduled run.** The job died. Nothing was lost
   only because it failed at 05:00 while I was watching. Had it expired on a
   Friday, I'd have found out Tuesday — past the retention floor.
2. **Windows Credential Manager lost the TextIt cookie.** Not expired — *absent*.
   `cmdkey /list` returned `* NONE *` and six consecutive runs failed. No audit
   trail, undiagnosable.
3. **Nothing could report that the job never ran.** Machine off, Docker down, task
   disabled → no alert, because nothing executed to send one.

A Cloud Run job's runtime service account does not expire, Secret Manager does not
silently drop secrets, and the freshness check below fires even when the job never
starts.

## What it does NOT fix

**A human still captures the cookie.** TextIt sits behind **AWS WAF** bot
protection: the login POST returns 202 with a JS challenge from `token.awswaf.com`,
and the resulting session is pre-auth, not authenticated. No header, no cookie, no
form field gets around that — it is exactly what it exists to prevent. Programmatic
login is impossible and this is settled; **do not re-probe it.**

So when TextIt invalidates the session, the job fails loudly and I re-capture the
cookie. This migration **relocates** that manual step from Credential Manager to
Secret Manager. It does not remove it.

---

## Architecture

```
Cloud Scheduler (hourly, :20, 05-23 America/Chicago)
    └─> Cloud Run job  webhook-log-ingest
            ├─ cookie  <- Secret Manager  textit-session-cookie:latest
            ├─ BigQuery <- runtime service account (no key, nothing to expire)
            └─ stdout  -> {"event":"ingest_failed","reason":"cookie_expired",...}
                              └─> log-based alert -> email

Cloud Scheduler (hourly, :50, 06-23 America/Chicago)   [SEPARATE CLOCK]
    └─> Cloud Run job  webhook-log-freshness
            └─ "when did BigQuery last receive a row?"
               stdout -> {"event":"freshness_failed","reason":"stale",...}
                              └─> log-based alert -> email
```

**The freshness check is a separate job on a separate schedule, and that is the
whole point.** A check that runs inside the ingest cannot fire when the ingest
never runs. `main.py` answers *"did the run work?"*; `freshness_check.py` answers
*"is the data actually current?"* — and the second has an honest answer whether the
ingest succeeded, failed, lied, or never started. Do not fold it into `main.py`.

Both alerts overlap on a loud failure and I get two emails. That's the correct
trade. The failure that must never be silent is the one where nothing fires at all.

| file | role |
|---|---|
| `src/main.py` | ingest job entrypoint; structured-log alerting |
| `src/freshness_check.py` | **separate job**; staleness + duplicate-key guard |
| `src/textit_webhook_failures.py` | httplog scraper + cookie from Secret Manager |
| `src/textit_webhook_ingest.py` | watermark, dedupe, MERGE, integrity assertions |
| `src/textit_webhook_detail.py` | detail-page parser + credential masking |

---

## Deploy

### 1. Create the repo as `lparker1119`

**This matters.** The Cloud Build GitHub App only reliably sees repos created by
that account — a repo created by any other account won't appear in the connect
dropdown, and `gcloud builds triggers create github` fails with
`FAILED_PRECONDITION: Repository mapping does not exist`. This is what stranded
`nightly-orchestrator`.

### 2. Runtime service account (least privilege)

A dedicated SA, not the default compute SA. Scoped to exactly what the job needs:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

$PROJ = "early-alert-responses"
$SA   = "webhook-log-ingest@$PROJ.iam.gserviceaccount.com"

gcloud iam service-accounts create webhook-log-ingest `
  --display-name="Webhook log ingest job" --project=$PROJ

# BigQuery: jobUser at PROJECT level (running a query needs it), but dataEditor
# scoped to the RESPONSES DATASET only -- not project-wide.
gcloud projects add-iam-policy-binding $PROJ `
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
```

Dataset-level `dataEditor` is not settable via `add-iam-policy-binding` — it's a
dataset ACL:

```powershell
bq show --format=prettyjson "$PROJ`:RESPONSES" > ds.json
# add to the "access" array, then:
#   { "role": "WRITER", "userByEmail": "webhook-log-ingest@early-alert-responses.iam.gserviceaccount.com" }
bq update --source=ds.json "$PROJ`:RESPONSES"
```

### 3. The cookie secret

```powershell
gcloud secrets create textit-session-cookie --replication-policy=automatic --project=$PROJ

gcloud secrets add-iam-policy-binding textit-session-cookie `
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"
```

Then add the value — see **Refreshing the cookie** below.

### 4. Cloud Build trigger

Connect the repo, branch `^main$`, config file `cloudbuild.yaml` (**not**
Dockerfile — this yaml deploys two jobs, it isn't just a build).

**The first build WILL fail** with `invalid value for build.service_account`
(INVALID_ARGUMENT, `gen1_repo_trigger`). It happens every single time. Fix: Cloud
Build → Triggers → open the trigger → set **Service account** to
`853176470965-compute@developer.gserviceaccount.com` → Save → Run.

The build SA also needs to be able to deploy jobs and act as the runtime SA:

```powershell
$BUILD = "853176470965-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJ `
  --member="serviceAccount:$BUILD" --role="roles/run.developer"

gcloud iam service-accounts add-iam-policy-binding $SA `
  --member="serviceAccount:$BUILD" --role="roles/iam.serviceAccountUser"
```

### 5. Smoke test — run each job by hand before scheduling anything

```powershell
gcloud run jobs execute webhook-log-ingest --region=us-east1 --wait
gcloud run jobs execute webhook-log-freshness --region=us-east1 --wait
```

Then **read the log line**, don't just check the exit code:

```powershell
gcloud logging read `
  'resource.type=cloud_run_job AND jsonPayload.event!=""' `
  --limit=5 --format="value(jsonPayload.event, jsonPayload.reason, jsonPayload.detail)"
```

Expect `ingest_ok` and `freshness_ok`. Only then attach Scheduler.

### 6. Cloud Scheduler

Scheduler triggers a **job** via the Cloud Run Admin API, not by POSTing to a URL.
It needs `roles/run.invoker` on the job.

**Note the 03:00–05:00 exclusion** — that window is reserved for Meridian's
Available Means ETL and Early Alert jobs must stay out of it.

```powershell
$SCHED_SA = "webhook-log-scheduler@$PROJ.iam.gserviceaccount.com"
gcloud iam service-accounts create webhook-log-scheduler --project=$PROJ

foreach ($job in @("webhook-log-ingest","webhook-log-freshness")) {
  gcloud run jobs add-iam-policy-binding $job --region=us-east1 `
    --member="serviceAccount:$SCHED_SA" --role="roles/run.invoker"
}

# INGEST -- hourly at :20, 05:00-23:00 CT. Same window the local task ran.
gcloud scheduler jobs create http webhook-log-ingest-hourly `
  --location=us-east1 `
  --schedule="20 5-23 * * *" `
  --time-zone="America/Chicago" `
  --uri="https://run.googleapis.com/v2/projects/$PROJ/locations/us-east1/jobs/webhook-log-ingest:run" `
  --http-method=POST `
  --oauth-service-account-email=$SCHED_SA

# FRESHNESS -- :50, offset 30 min AFTER the ingest so it reads a settled table.
# Starts at 06 so the first check follows a real ingest rather than alerting on
# an overnight gap that is expected.
gcloud scheduler jobs create http webhook-log-freshness-hourly `
  --location=us-east1 `
  --schedule="50 6-23 * * *" `
  --time-zone="America/Chicago" `
  --uri="https://run.googleapis.com/v2/projects/$PROJ/locations/us-east1/jobs/webhook-log-freshness:run" `
  --http-method=POST `
  --oauth-service-account-email=$SCHED_SA
```

Use `--time-zone`, never a fixed-UTC cron — a UTC cron drifts an hour across DST.

---

## Alerting — "how will I see this when it fails at 5am?"

Answering that literally is a hard requirement before anything unattended ships,
so:

Every terminal state writes **one structured JSON line** to stdout. Cloud Logging
lifts a bare JSON object into `jsonPayload`. Log-based alert policies watch it and
email me, with the cause **in the subject line**.

```
jsonPayload.event   ingest_ok | ingest_failed | freshness_ok | freshness_failed
jsonPayload.reason  cookie_expired | parser_stale | bq_auth | integrity | stale | duplicates
jsonPayload.detail  the child's REAL output (last 4000 chars)
```

`detail` carrying the child's actual stdout is deliberate. The local wrapper's
alert email showed only a PowerShell stack trace while the real diagnosis —
`BigQuery auth failed. If ADC expired, re-run: gcloud auth application-default
login` — was printed by the child and thrown away. An alert that can't say what
broke has only moved the debugging to a worse moment.

### Create the two policies

```powershell
gcloud logging metrics create webhook_ingest_failures `
  --description="Terminal failures of the webhook-log ingest or freshness job" `
  --log-filter='resource.type="cloud_run_job" AND (jsonPayload.event="ingest_failed" OR jsonPayload.event="freshness_failed")'
```

Then Monitoring → Alerting → Create policy on that metric, condition **any value >
0**, notification channel = my email.

**Do not rebuild the de-duplication logic in Python.** The PowerShell wrapper kept
a JSON state file to email once per failure streak, re-alert every 6h, and send one
RECOVERED notice. That was ~80 lines and was rewritten three times (it reported
FAILURE on successful runs, then UNVERIFIED on successful runs, then couldn't say
what broke when it genuinely failed). A Monitoring policy does all four natively:

| PowerShell behavior | Monitoring equivalent |
|---|---|
| email on first failure only | one incident opens; no re-notify |
| re-alert every 6h | `notification_rate_limit: 21600s` |
| one-time RECOVERED notice | `auto_close` — Monitoring emails on close |
| record every run regardless | every run is in Cloud Logging |

### Third guard: metric-absence

The two policies above fire when a job *runs and fails*. Add one that fires when
the ingest **stops producing any line at all** — the "job never ran" case, from the
other side:

Monitoring → Create policy → condition type **Metric absence** on a log metric
counting `jsonPayload.event="ingest_ok"`, trigger after **3 hours**.

Belt and braces with `freshness_check.py`, on purpose. Metric-absence catches *"the
job never ran."* Freshness catches *"the job ran, exited 0, and moved no data"* —
which is the Meridian ETL Defect-B signature, where two Required steps failed and
the pipeline still emailed SUCCESS. Neither is sufficient alone.

---

## Refreshing the cookie

The job exits **2** with `reason: cookie_expired` and the alert names it. Then:

1. Log in to `textit.com` in a browser.
2. DevTools → Application → Cookies → copy the full `Cookie` header value
   (`sessionid=...; csrftoken=...`).
3. Add a new secret version — the job reads `:latest`, so **no redeploy**:

```powershell
gcloud secrets versions add textit-session-cookie --data-file=- --project=early-alert-responses
# paste the cookie, then Ctrl-Z + Enter (Windows)
```

4. Re-run immediately rather than waiting for the next hour:

```powershell
gcloud run jobs execute webhook-log-ingest --region=us-east1 --wait
```

The ingest is idempotent (MERGE on `httplog_id`), so it picks up everything the
failed runs missed. No manual catch-up.

---

## What must not be lost if this is ever rewritten

All of it was learned the hard way and all of it is load-bearing.

- **Two modes.** Incremental **stops at** `MAX(httplog_id)`; backfill **steps
  over** stored rows. The watermark is a *ceiling* — incremental mode structurally
  **cannot** backfill. A 200-page backfill once ingested 27 rows and exited in two
  minutes because it hit the watermark on page 1 and broke.
- **MERGE does not dedupe its own source.** The scraper pages a *live* list while
  webhooks fire (~10/min), so a row can appear on two pages and both take
  `WHEN NOT MATCHED` and both INSERT. Dedupe the batch **before** staging. The
  standing `COUNT(*) == COUNT(DISTINCT httplog_id)` assertion runs on every write.
- **A failed detail fetch must DEFER the whole pair.** Never write a list row
  without its body — the watermark advances past it and the body can never be
  re-fetched. A permanent, invisible hole.
- **404 past the last page is the normal end of the log** (~1,600 pages), not an
  error. Treating it as fatal crashed a backfill at page 1600 and discarded 1,599
  pages of work.
- **Checkpoint every 500 rows.** A crash mid-crawl must not discard the run.
- **Credentials are masked at ingest**, in headers *and* bodies, irreversibly.
  `webhook_log_detail` is a PII store regardless.
- **Fail non-zero rather than writing zero rows.** A stale parser after a TextIt
  reskin would otherwise look like a quiet day — and under 3.85-day retention, a
  quiet day noticed on day five is permanent loss.
- **`PYTHONUNBUFFERED=1`.** Without it, stdout buffers and nothing reaches Cloud
  Logging until the process exits, so a hung job looks identical to one that never
  started.

## This repo is PUBLIC

No credentials in source, in **any** form — not in code, not as a default value,
not in a docstring example, not in a committed `.pyc`. All four of those happened
in `sheet-service` and the password was world-readable from its first commit.

The cookie lives in Secret Manager. It does **not** go in a Cloud Run env var
either: env vars are plaintext to anyone with `roles/run.viewer` via a read-only
`gcloud run services describe`, and this cookie is a live TextIt session with
console access to subscriber PII.
