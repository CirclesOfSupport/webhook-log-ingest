#!/usr/bin/env python3
"""
main.py — Cloud Run JOB entrypoint for the TextIt webhook-log ingest.

This is a JOB, not a service. There is no Flask app and no HTTP handler: it runs
to completion and exits. Cloud Scheduler triggers it via the Cloud Run Admin API
(`jobs.run`), not by POSTing to a URL.

    Job:        webhook-log-ingest       (us-east1)
    Trigger:    Cloud Scheduler, hourly, MINUTE 20, hours 05-23 America/Chicago
                (avoids 03:00-05:00, reserved for Meridian's Available Means ETL)
    Cookie:     Secret Manager  textit-session-cookie:latest
    BQ auth:    the job's runtime service account


HOW LOGAN SEES THIS WHEN IT FAILS AT 5AM
========================================
This is the question CONVENTIONS.md requires answering literally before shipping
anything unattended, so: answering it literally.

Every terminal state of this job writes a single structured JSON line to stdout.
Cloud Run pipes stdout to Cloud Logging, and Logging parses a bare JSON object
into `jsonPayload`. Two log-based alert policies watch that field and email Logan.

    jsonPayload.event = "ingest_failed"    -> ALERT "Early Alert webhook ingest FAILED"
    jsonPayload.event = "ingest_ok"        -> no alert; this is the heartbeat

`jsonPayload.reason` carries the diagnosis (cookie_expired / parser_stale /
bq_auth / integrity / unknown), and the alert policy puts it in the SUBJECT --
exactly what the PowerShell wrapper's subject-line switch did, because an alert
that cannot name the cause has only moved the debugging to a worse moment.

WHAT PORTS FROM THE POWERSHELL WRAPPER, AND WHAT DOES NOT
--------------------------------------------------------
The wrapper's alert de-duplication (email once on first failure, stay quiet while
it persists, re-alert every 6h, one-time RECOVERED notice) was ~80 lines of
PowerShell keeping a JSON state file on disk. NONE of that ports, and none of it
needs to: a Cloud Monitoring alert policy does all four natively.

    first-failure-only   -> a policy opens ONE incident and does not re-notify
    6h re-alert          -> notification_rate_limit on the policy
    RECOVERED notice     -> auto_close; Monitoring emails when the incident closes
    "record every run"   -> every run is in Cloud Logging regardless

Do NOT rebuild the state machine in Python. It was rewritten three times locally
(it reported FAILURE on successful runs, then UNVERIFIED on successful runs, then
could not say what broke when it genuinely failed). The platform has it.

WHAT THIS JOB DELIBERATELY DOES NOT DO: THE FRESHNESS CHECK
-----------------------------------------------------------
The local wrapper verified freshness against BigQuery AFTER a successful ingest.
That check is NOT here, and its absence is the point.

A check that runs inside the job it is checking cannot fire when the job never
runs -- machine off, job deleted, scheduler paused, quota exhausted. That is the
one remaining way to silently lose data under a 3.85-day retention floor, and it
is precisely what the local setup could not close.

So freshness moved OUT, into a separate scheduled BigQuery query that alerts on
MAX(fired_at) falling behind. It runs on its own clock and knows nothing about
this job. It fires whether this job fails, succeeds wrongly, or never starts.
See freshness_check.sql + the deploy runbook in README.md.

    This job answers:      "did the run work?"
    The freshness check:   "did a run HAPPEN, and is the data actually current?"

Those are different questions and they must not share a fate.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback

# Unbuffered stdout. Without this, Python buffers and NOTHING appears in Cloud
# Logging until the process exits -- so a job that hangs looks identical to a job
# that never started. (The Dockerfile also sets PYTHONUNBUFFERED=1; belt and braces.)
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

# Default ingest window. Incremental mode stops at MAX(httplog_id) anyway, so this
# is a safety ceiling on paging, not a filter: at ~15k rows/day and 25 rows/page,
# an hour of traffic is ~25 pages. 120 gives ~5h of slack for a missed run or two
# without ever letting a runaway crawl hammer TextIt's UI.
DEFAULT_MAX_PAGES = int(os.getenv("MAX_PAGES", "120"))

# Exit codes, inherited from the local scripts. The alert policy keys on `reason`,
# not on these, but they stay stable so a human reading the Cloud Run execution
# list can tell states apart at a glance.
#   1 = generic abort (die())     2 = cookie missing/expired
#   3 = parser produced zero rows 4 = duplicate httplog_ids
#   5 = list/detail row mismatch


def emit(event: str, **fields) -> None:
    """
    One structured JSON line to stdout. Cloud Logging lifts a bare JSON object
    straight into jsonPayload, which is what the alert policies filter on.

    `severity` is a magic key Cloud Logging understands -- it sets the log entry's
    level, so a failure shows up RED in the console without any extra config.
    """
    rec = {"event": event, **fields}
    rec.setdefault("severity", "ERROR" if event.endswith("_failed") else "INFO")
    print(json.dumps(rec), flush=True)


def classify(output: str, exit_code: int) -> str:
    """
    Name the cause, so the alert email can name it too.

    This is a direct port of the PowerShell wrapper's subject-line switch. Same
    strings, same order, same reasoning: Logan should learn what broke from the
    subject line, not by opening a log under a retention clock.
    """
    o = output or ""
    if "BigQuery auth failed" in o or "Reauthentication is needed" in o:
        return "bq_auth"
    if ("session cookie is missing or expired" in o
            or "Could not read secret" in o
            or "session cookie is EMPTY" in o
            or exit_code == 2):
        return "cookie_expired"
    if "DetailParseError" in o or "parser is stale" in o or "HTML has" in o or exit_code == 3:
        return "parser_stale"
    if "INTEGRITY FAILURE" in o or exit_code in (4, 5):
        return "integrity"
    return "unknown"


class Tee:
    """
    Fan stdout AND stderr into ONE shared buffer, while still writing each through
    to its real stream (and thus to Cloud Logging, live).

    ONE buffer, not two, because the failure record wants the diagnosis in the
    order it was actually printed -- progress and the abort message interleaved as
    they happened, not two disjoint blocks the reader has to reassemble.

    Tee, do not redirect. On 2026-07-13 the child's diagnosis existed and reached
    Logan through NONE of four surfaces, because the local wrapper captured
    PowerShell's error record (a stack trace) and discarded the child's real output.
    The text must go to the log live AND be retained for the alert body.
    """

    class _Side:
        def __init__(self, parent, stream):
            self.parent, self.stream = parent, stream

        def write(self, s):
            self.parent.buf.append(s)
            self.stream.write(s)

        def flush(self):
            self.stream.flush()

        # Some libraries probe these before writing.
        def isatty(self):
            return False

        @property
        def encoding(self):
            return getattr(self.stream, "encoding", "utf-8")

    def __init__(self, stdout, stderr):
        self.buf: list[str] = []
        self.out = Tee._Side(self, stdout)
        self.err = Tee._Side(self, stderr)

    def text(self) -> str:
        return "".join(self.buf)


def main() -> int:
    started = time.time()

    # Imported here, not at module scope: an import-time failure (missing dep, bad
    # env) must be caught by the try/except below and reported as a structured
    # ingest_failed, not crash the interpreter before any logging exists.
    from textit_webhook_failures import make_session, BASE, row_time
    import textit_webhook_failures as tf
    from textit_webhook_ingest import run_ingest

    # TEE BOTH STREAMS INTO ONE BUFFER.
    #
    # stderr is NOT optional here. Every module in this pipeline prints its actual
    # DIAGNOSIS to stderr -- die() writes "!!! INGEST ABORTED / BigQuery auth
    # failed..." to stderr; the cookie failure writes "Could not read secret..." to
    # stderr; the parser-stale abort writes to stderr. stdout carries only progress.
    #
    # Teeing stdout alone reproduces the 2026-07-13 failure EXACTLY: the alert fires,
    # classifies correctly, and arrives with an EMPTY detail field -- so it says
    # something broke and cannot say what. That is the precise defect this migration
    # exists to remove. Caught by running main.py with no cookie and reading the
    # emitted line, not by inspecting the code.
    tee = Tee(sys.stdout, sys.stderr)
    real_stdout, real_stderr = sys.stdout, sys.stderr
    sys.stdout = tee.out
    sys.stderr = tee.err

    try:
        session = make_session()

        # Page the httplog newest-first and collect rows. This mirrors the CLI's
        # --ingest path (include_success=True: we capture EVERY fire, not just
        # failures -- webhook_log is the volume/success-rate record, not a failure
        # log). Incremental mode inside run_ingest() stops at the watermark.
        matches: list[dict] = []
        scanned = 0
        page = 0

        while page < DEFAULT_MAX_PAGES:
            page += 1
            try:
                html = tf.get_page(session, page)
            except tf.EndOfLog:
                # 404 past the last page is the NORMAL end of the log (~1,600
                # pages), not an error. Treating it as fatal is what discarded
                # 1,599 pages of a backfill on 2026-07-11.
                print(f"[paging] reached end of TextIt log at page {page}")
                break

            rows, _total = tf.parse_rows(html)
            if not rows:
                break
            scanned += len(rows)
            matches.extend(rows)

        # STRUCTURAL SELF-CHECK. The scraper has no API contract. A TextIt reskin
        # yields zero parseable rows, and writing zero rows into a table people
        # trust is WORSE than failing -- under 3.85-day retention, a gap noticed on
        # day five is unrecoverable. So a stale parser must fail loud, not look
        # like a quiet day.
        ing_rows = []
        for r in matches:
            hid = r.get("httplog_id")
            rt = row_time(r)
            if hid is None or rt is None:
                continue
            ing_rows.append({
                "httplog_id": hid,
                "fired_at": rt,
                "status_code": r["status_code"],
                "elapsed_ms": r["elapsed_ms"],
                "webhook_url": r["webhook_url"],
                "flow_name": r["flow_name"],
                "flow_uuid": r["flow_uuid"],
            })

        if scanned > 0 and not ing_rows:
            print("*** INGEST ABORTED: scraped %d rows but parsed ZERO usable ones."
                  % scanned, file=sys.stderr)
            print("    TextIt's HTML has almost certainly changed. Nothing written.",
                  file=sys.stderr)
            sys.stdout, sys.stderr = real_stdout, real_stderr
            emit("ingest_failed", reason="parser_stale", exit_code=3,
                 rows_scanned=scanned, detail=tee.text()[-4000:])
            return 3

        ing_rows.sort(key=lambda r: r["httplog_id"], reverse=True)  # newest-first

        # run_ingest() carries the load-bearing behavior unchanged: incremental
        # watermark, source-batch dedupe before MERGE, DEFER-the-pair on a failed
        # detail fetch, 500-row checkpoints, and the COUNT(*) == COUNT(DISTINCT)
        # integrity assertion. It exits non-zero via die()/sys.exit on any of them.
        run_ingest(session, ing_rows, BASE, DEFAULT_MAX_PAGES,
                   dry_run=False, backfill=False)

    except SystemExit as e:
        # die() and the integrity assertions exit non-zero. Catch it so the failure
        # is REPORTED structurally rather than just killing the process silently.
        code = e.code if isinstance(e.code, int) else 1
        sys.stdout, sys.stderr = real_stdout, real_stderr
        if code == 0:
            emit("ingest_ok", pages=page, rows_scanned=scanned,
                 elapsed_sec=round(time.time() - started, 1))
            return 0
        out = tee.text()
        emit("ingest_failed", reason=classify(out, code), exit_code=code,
             elapsed_sec=round(time.time() - started, 1), detail=out[-4000:])
        return code

    except Exception:
        sys.stdout, sys.stderr = real_stdout, real_stderr
        tb = traceback.format_exc()
        emit("ingest_failed", reason=classify(tee.text() + tb, 1), exit_code=1,
             elapsed_sec=round(time.time() - started, 1),
             detail=(tee.text() + "\n" + tb)[-4000:])
        return 1

    sys.stdout, sys.stderr = real_stdout, real_stderr
    emit("ingest_ok", pages=page, rows_scanned=scanned,
         elapsed_sec=round(time.time() - started, 1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
