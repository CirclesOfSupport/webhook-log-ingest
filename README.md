# webhook-log-ingest

Cloud Run **service** that ingests the TextIt webhook HTTP log into BigQuery
(`RESPONSES.webhook_log` + `webhook_log_detail`). Called hourly by Cloud
Scheduler. Replaces a Windows Scheduled Task that ran on Logan's workstation.

Same shape as the other Early Alert services (`vamc-sync`, `contacts-sync`):
Flask app, `/health` + a work endpoint, deployed from this repo via Cloud Build,
`--no-allow-unauthenticated` + IAM.

## Endpoints

- `GET /health` -> `{"status":"ok"}`
- `POST /run` -> pages the log, MERGEs new rows, returns
  `{"status":"success","pages":N,"rows_scanned":N,"ingested":N}`

Incremental: walks the httplog newest-first and stops at the stored
`MAX(httplog_id)` watermark. Idempotent -- MERGE on `httplog_id`, so a double run
cannot duplicate a row.

## Why it exists / why it moved off the local machine

TextIt exposes **no API** for the webhook HTTP log (24 endpoints advertised;
`http_logs` and `webhook_events` both 404). The only access is the authenticated
HTML console, so this pages it and parses it. TextIt retains only **~3.85 days**
of that log -- anything not ingested in that window is deleted at the source and
is unrecoverable, which is why a stale parser fails loud rather than writing zero
rows.

The local version authenticated to BigQuery with **user ADC**, which expired on
its first scheduled run. The runtime service account does not expire.

## Auth

`--no-allow-unauthenticated`. The only caller is Cloud Scheduler, which mints a
Google OIDC token -- IAM gates the door, per the stack rule that auth follows the
caller. No app-layer shared secret is needed (`SYNC_PASSWORD` is an optional
second layer, left unset). Grant the Scheduler SA `roles/run.invoker`.

## The TextIt session cookie

The scraper needs a logged-in browser session cookie. Programmatic login is
blocked by AWS WAF, so a human captures it; it lives in **Secret Manager**
(`textit-session-cookie`, read at `:latest` every run -- refresh is one
`gcloud secrets versions add`, no redeploy). The runtime SA needs
`roles/secretmanager.secretAccessor` on that secret.

To refresh after a `cookie_expired` failure: log in to textit.com, DevTools ->
Application -> Cookies, copy the Cookie header, then
`gcloud secrets versions add textit-session-cookie --data-file=-`.

## Deploy (per cloud_run_service_deploy runbook)

Cloud Run -> **Services** -> **Connect repo** -> Continuously deploy from a
repository -> Cloud Build -> select `CirclesOfSupport/webhook-log-ingest` ->
branch `^main$`, build type **Dockerfile** -> service name `webhook-log-ingest`,
region **us-east1**, **Require authentication** -> set env vars in the
**Variables & Secrets** tab BEFORE Create.

- **First build fails** with `invalid value for build.service_account`. Set the
  trigger's Service account to `853176470965-compute@developer.gserviceaccount.com`,
  Save, Run. Then it builds clean.
- **Env vars and memory go in the console, not `cloudbuild.yaml`** -- the
  auto-trigger ignores those flags in the yaml (the contacts-sync OOM). This
  repo's yaml deliberately omits them.

### Env vars (console)

- `SECRET_PROJECT=early-alert-responses`
- `TEXTIT_COOKIE_SECRET=textit-session-cookie`
- `MAX_PAGES=120`
- (`SYNC_PASSWORD` -- leave unset; IAM is the gate)

### Runtime SA roles (on early-alert-responses)

`roles/bigquery.dataEditor` + `roles/bigquery.jobUser` +
`roles/secretmanager.secretAccessor` (on the cookie secret).

## Smoke test before scheduling (PowerShell)

```powershell
$token = gcloud auth print-identity-token
Invoke-RestMethod -Uri "https://<service-url>/health" -Headers @{ Authorization = "Bearer $token" }

$token = gcloud auth print-identity-token
Invoke-RestMethod -Uri "https://<service-url>/run" -Method POST `
  -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body '{}'
```

Expect `status: success`. Only then attach Scheduler.

## Cloud Scheduler

Create Job -> region us-east1 -> `20 5-23 * * *` (or your chosen cadence),
time-zone **America/Chicago** -> target HTTP, the service URL + `/run`, method
POST, body `{}` -> add an OIDC token (the Scheduler SA). Force-run once to
confirm.

## Alerting (add later -- not required to go live)

Failures emit a structured log line: `jsonPayload.event="ingest_failed"` at ERROR
severity, with `jsonPayload.reason` (cookie_expired / parser_stale / bq_auth /
integrity / unknown) and `jsonPayload.detail`.

To email on failure: create a log-based metric on that filter, then a Cloud
Monitoring alert policy with an email notification channel. First-failure-only,
re-alert throttle, and recovery notice are native to the policy. **No code change
needed** -- the log line is already emitted.

## This repo is PUBLIC

No credentials in source, in any form (code, default value, docstring example,
committed `.pyc`). The cookie is in Secret Manager; env vars are set in the
console, not committed.

## What must not be lost if rewritten

Incremental **stops at** the watermark (a ceiling) -- it structurally cannot
backfill; backfill is a separate `run_ingest(backfill=True)` path that steps over
stored rows. MERGE does **not** dedupe its own source (live-list page drift ->
duplicate `httplog_id`s; dedupe the batch before staging; `COUNT(*)==COUNT(DISTINCT)`
asserted every write). A failed detail fetch **DEFERs the whole pair** (a list row
without a body is a permanent hole once the watermark passes). 404 past the last
page is the normal end of the log. Credentials masked at ingest, headers and
bodies. **Fail non-zero rather than write zero rows.**
