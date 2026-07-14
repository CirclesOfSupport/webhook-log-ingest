#!/usr/bin/env python3
"""
freshness_check.py — the "did a run actually HAPPEN" guard.

SEPARATE CLOUD RUN JOB. Deployed from this same repo, different entrypoint,
different schedule, different Scheduler trigger. It is NOT part of main.py and
must never be folded into it.

WHY IT IS SEPARATE (this is the whole reason the handoff exists)
================================================================
The local pipeline verified freshness at the END of a successful ingest. That
check could prove the job FAILED. It could not prove the job HAPPENED.

Machine off, Docker down, task disabled, network out, scheduler paused, job
deleted, quota exhausted -> the ingest never executes -> NOTHING RUNS TO SEND AN
ALERT. Silence. And TextIt retains ~3.85 days, so silence for a long weekend is
permanent, unrecoverable data loss at the source.

A check that lives inside the thing it checks shares that thing's fate. This one
does not. It runs on its own clock, holds no reference to the ingest job, and
fires on the DATA, not on the job's self-report:

    "when did BigQuery last receive a webhook row?"

That question has an honest answer whether the ingest succeeded, failed, lied, or
never started. It is the only formulation that closes the gap.

    main.py           answers  "did the run work?"          (fires on failure)
    freshness_check   answers  "is the DATA current?"        (fires on ABSENCE)

Two alerts, deliberately overlapping. If the ingest dies loudly, both fire and
Logan gets a redundant email -- that is the correct trade. The failure that must
never be silent is the one where nothing fires at all.

BELT AND BRACES: a Cloud Monitoring "metric absence" policy on the ingest_ok log
line covers the same gap from the other side (see README). Neither is sufficient
alone. Metric-absence catches "job never ran"; THIS catches "job ran, exited 0,
and moved no data" -- the Meridian ETL Defect-B signature, where two Required
steps failed and the pipeline still reported SUCCESS.

WHY NOT A BIGQUERY SCHEDULED QUERY: a scheduled query can compute staleness but
cannot email on a condition -- it would need a result table plus something else
watching it, i.e. exactly this, with an extra moving part. A tiny Cloud Run job
that emits a structured log line plugs straight into the same alert policy
machinery as the ingest, with no new surface.
"""
from __future__ import annotations

import json
import os
import sys

from google.cloud import bigquery

PROJECT = "early-alert-responses"
T_LOG = f"{PROJECT}.RESPONSES.webhook_log"

# Alert if the newest row is older than this.
#
# The ingest runs HOURLY. 3h = one missed run plus slack -- tight enough that a
# broken credential is caught the same morning, loose enough that a single blip
# does not page anyone.
#
# It must stay FAR below TextIt's ~3.85-day (92h) retention floor. That floor is
# the actual deadline: past it the rows are deleted at the source and no amount
# of alerting recovers them. 3h leaves ~89h of runway to notice and fix.
STALE_HOURS = int(os.getenv("STALE_HOURS", "3"))

SQL = f"""
SELECT
  COUNT(*)                    AS row_count,
  COUNT(DISTINCT httplog_id)  AS distinct_ids,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(fired_at), HOUR) AS hours_stale,
  MAX(fired_at)               AS newest_row
FROM `{T_LOG}`
"""


def emit(event: str, **fields) -> None:
    rec = {"event": event, **fields}
    rec.setdefault("severity", "ERROR" if event.endswith("_failed") else "INFO")
    print(json.dumps(rec), flush=True)


def main() -> int:
    try:
        client = bigquery.Client(project=PROJECT)
        r = list(client.query(SQL).result())[0]
    except Exception as exc:
        # The check itself broke. This must ALSO alert -- a freshness check that
        # dies quietly is a freshness check that is not running, which is the
        # exact condition it exists to detect. Never fail closed into silence.
        emit("freshness_failed", reason="query_error", detail=str(exc)[:2000])
        return 1

    stale = r.hours_stale if r.hours_stale is not None else -1
    problems = []

    if r.row_count != r.distinct_ids:
        # httplog_id is the MERGE key. Duplicates mean the source-batch dedupe is
        # broken -- the pipeline is corrupting its own key, which is worse than a
        # pipeline that fails. This is the invariant that only surfaced in 2026-07
        # because Logan eyeballed COUNT(*) vs COUNT(DISTINCT); it is asserted on
        # every run now, from two places.
        problems.append(
            f"DUPLICATES: {r.row_count} rows but only {r.distinct_ids} distinct httplog_ids "
            f"({r.row_count - r.distinct_ids} duplicate(s)). Run dedupe_webhook_log.sql."
        )

    if stale < 0:
        problems.append("EMPTY: webhook_log has no rows at all.")
    elif stale > STALE_HOURS:
        problems.append(
            f"STALE: newest row is {stale}h old (threshold {STALE_HOURS}h). "
            f"The ingest has not moved data. TextIt retains ~92h -- "
            f"approximately {max(0, 92 - stale)}h of runway remain before this window "
            f"is deleted at the source and is UNRECOVERABLE."
        )

    if problems:
        emit(
            "freshness_failed",
            reason="stale" if any(p.startswith(("STALE", "EMPTY")) for p in problems) else "duplicates",
            hours_stale=stale,
            row_count=r.row_count,
            distinct_ids=r.distinct_ids,
            newest_row=str(r.newest_row),
            detail=" | ".join(problems),
        )
        return 1

    emit(
        "freshness_ok",
        hours_stale=stale,
        row_count=r.row_count,
        distinct_ids=r.distinct_ids,
        newest_row=str(r.newest_row),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
