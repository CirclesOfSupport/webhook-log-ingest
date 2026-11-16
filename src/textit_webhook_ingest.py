#!/usr/bin/env python3
"""
textit_webhook_ingest.py
========================
Incremental ingest of the TextIt webhook HTTP log into BigQuery.

    OPS.webhook_log         list rows, indefinite retention
    OPS.webhook_log_detail  headers + bodies, 30d native partition expiry

WHERE THIS RUNS: Cloud Run SERVICE `webhook-log-ingest`, hourly via a Cloud Scheduler
    job that POSTs to /run.
    Migrated off Logan's workstation 2026-07-14 (was a Windows Scheduled Task).

    TextIt exposes NO API for the webhook HTTP log (TESTED 2026-07-11: the
    workspace advertises 24 endpoints; http_logs / webhook_events both 404), and
    programmatic login is blocked by AWS WAF (TESTED 2026-07-13). So the session
    cookie is still captured by a HUMAN -- but it now lives in Secret Manager
    rather than Windows Credential Manager, and the BigQuery credential is the
    service's runtime service account rather than user ADC. Both of the credentials
    that failed in production are addressed; the manual cookie capture is
    relocated, not removed. See src/textit_webhook_failures.py get_cookie().

WHY DAILY IS A HARD REQUIREMENT (not a preference):
    TextIt retains only ~3.85 DAYS of httplog (TESTED: 55,263 rows / 2,211
    pages; oldest row 92.5h old). Anything not ingested inside that window is
    deleted by TextIt permanently and is unrecoverable. A silent ingest failure
    that goes unnoticed for four days is a permanent hole in the record.
    Hence: this script FAILS LOUD AND NON-ZERO rather than writing zero rows.

IDEMPOTENCY:
    Both writes are MERGE ... ON httplog_id. Re-running over an overlapping
    window is safe and produces no duplicates. httplog_id is TextIt's own row
    id, parsed from /httplog/read/<id>/, and is monotonic.

INCREMENTAL WATERMARK:
    SELECT MAX(httplog_id) FROM webhook_log, then page the list newest-first
    and stop on the first id <= watermark. First run (empty table) has no
    watermark and pages to --max-pages.

BATCH LOAD, NOT STREAMING:
    Rows go via load_table_from_json into a temp table, then MERGE. The BQ
    streaming API bills $0.05/GB; batch loads are free. At ~15k rows/day this
    is the difference between free and not.
"""
from __future__ import annotations

import sys
import time
import datetime as dt
from typing import Iterable

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from textit_webhook_detail import (
    parse_detail,
    status_code_from_status_line,
    DetailParseError,
)

PROJECT = "early-alert-responses"
DATASET = "OPS"
T_LOG = f"{PROJECT}.{DATASET}.webhook_log"
T_DETAIL = f"{PROJECT}.{DATASET}.webhook_log_detail"

# Politeness delay between detail fetches. The backfill is ~55k requests against
# a vendor UI that was never meant to be crawled; do not hammer it.
DETAIL_DELAY_SEC = 0.15

# Flush to BigQuery every N detail fetches instead of accumulating the whole run
# and MERGEing once at the end.
#
# WHY: a single terminal MERGE means ANY failure -- a 404 past the last page, an
# expired cookie, a network blip 90 minutes into a 2,400-page crawl -- discards
# every row collected. That happened 2026-07-11: the run died at page 1600 and
# threw away ~1,599 pages of work.
#
# MERGE is idempotent on httplog_id, so partial progress is always safe to keep
# and a re-run simply resumes. Checkpointing converts a total loss into a partial
# success.
CHECKPOINT_EVERY = 500


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def bq_client() -> bigquery.Client:
    """
    Ambient service-account credentials (ADC) -- the Cloud Run runtime service
    account. Same pattern as every other service in the stack (vamc-sync,
    contacts-sync). No key file, no user ADC, nothing to expire.

    Runtime SA needs, on early-alert-responses:
        roles/bigquery.dataEditor + roles/bigquery.jobUser
    """
    return bigquery.Client(project=PROJECT)


class IngestAborted(RuntimeError):
    """Raised by die(). An exception, not sys.exit(): inside the gunicorn worker a
    SystemExit is not caught by the /run handler's `except Exception`, so the worker
    dies, the request ends 503 and no ingest_failed line is written for the alert."""


def die(msg: str) -> None:
    """Fail loud, fail non-zero. Never fail into a silent zero-row ingest."""
    print(f"\n!!! INGEST ABORTED\n{msg}\n", file=sys.stderr)
    raise IngestAborted(msg)


def get_watermark(client: bigquery.Client) -> int | None:
    """Highest httplog_id already stored. None on an empty table (first run)."""
    row = list(client.query(f"SELECT MAX(httplog_id) AS hw FROM `{T_LOG}`").result())[0]
    return int(row.hw) if row.hw is not None else None


def get_stored_ids(client: bigquery.Client, lo: int, hi: int) -> set[int]:
    """
    httplog_ids already stored, within [lo, hi].

    Backfill uses this to SKIP THE DETAIL FETCH for rows it already has, while
    still paging PAST them into older history. This is the crucial difference
    from incremental mode: incremental STOPS at a stored row; backfill STEPS OVER it.
    """
    sql = f"""
    SELECT httplog_id FROM `{T_LOG}`
    WHERE httplog_id BETWEEN {int(lo)} AND {int(hi)}
    """
    return {int(r.httplog_id) for r in client.query(sql).result()}


def merge_rows(
    client: bigquery.Client,
    target: str,
    rows: list[dict],
    schema: list[bigquery.SchemaField],
    update_cols: list[str],
) -> int:
    """
    Batch-load rows to a temp table, then MERGE on httplog_id.

    MERGE (not INSERT) so re-running an overlapping window cannot duplicate.
    Note the UPDATE clause names only the columns we actually write — BQ bills
    per column touched, and the 84-column UPDATE SET in get-responses_v2 is
    precisely the mistake that produced this project's BQ overspend. Keep it
    narrow.
    """
    if not rows:
        return 0

    # ---- DEDUPE THE SOURCE BATCH. THIS IS LOAD-BEARING. ----
    # BigQuery MERGE deduplicates against the TARGET, not within the SOURCE.
    # If the same httplog_id appears twice in the staging table and neither
    # matches an existing target row, BOTH take WHEN NOT MATCHED and BOTH
    # INSERT -- producing duplicates from a statement that looks idempotent.
    #
    # The scraper DOES produce duplicate ids: it pages a live, newest-first
    # list while new webhooks are firing, so rows shift down between page
    # fetches and a row seen on page N can reappear on page N+1.
    #
    # Found 2026-07-11: first 5,000-row backfill chunk landed 5000 rows /
    # 4992 distinct ids -- 8 dupes. The earlier idempotency test missed this
    # because it re-offered rows pulled FROM BigQuery, which are distinct by
    # construction. Keep last occurrence (later page = same row, no loss).
    seen: dict[int, dict] = {}
    for r in rows:
        seen[r["httplog_id"]] = r
    if len(seen) != len(rows):
        print(f"  [dedupe] source batch had {len(rows)} rows, "
              f"{len(seen)} distinct httplog_ids -- dropped {len(rows) - len(seen)} "
              f"duplicate(s) from live-list page drift")
    rows = list(seen.values())

    tmp_id = f"{PROJECT}.{DATASET}._tmp_{target.split('.')[-1]}_{int(time.time())}"
    tmp = bigquery.Table(tmp_id, schema=schema)
    tmp.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    client.create_table(tmp)

    try:
        job = client.load_table_from_json(
            rows,
            tmp_id,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        job.result()  # raises on failure

        cols = [f.name for f in schema]
        set_clause = ",\n    ".join(f"T.`{c}` = S.`{c}`" for c in update_cols)
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join(f"S.`{c}`" for c in cols)

        sql = f"""
        MERGE `{target}` T
        USING `{tmp_id}` S
        ON T.httplog_id = S.httplog_id
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        client.query(sql).result()
        return len(rows)
    finally:
        client.delete_table(tmp_id, not_found_ok=True)


LOG_SCHEMA = [
    bigquery.SchemaField("httplog_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("fired_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("status_code", "INT64"),
    bigquery.SchemaField("is_failure", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("elapsed_ms", "INT64"),
    bigquery.SchemaField("webhook_url", "STRING"),
    bigquery.SchemaField("flow_name", "STRING"),
    bigquery.SchemaField("flow_uuid", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

DETAIL_SCHEMA = [
    bigquery.SchemaField("httplog_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("fired_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("request_method", "STRING"),
    bigquery.SchemaField("request_path", "STRING"),
    bigquery.SchemaField("request_host", "STRING"),
    bigquery.SchemaField("request_headers", "STRING"),
    bigquery.SchemaField("request_body", "STRING"),
    bigquery.SchemaField("response_status_line", "STRING"),
    bigquery.SchemaField("response_headers", "STRING"),
    bigquery.SchemaField("response_body", "STRING"),
    bigquery.SchemaField("detail_fetched_at", "TIMESTAMP", mode="REQUIRED"),
]

# Do not overwrite ingested_at / detail_fetched_at on re-ingest of the same row —
# they record when we FIRST captured it.
LOG_UPDATE_COLS = [
    "fired_at", "status_code", "is_failure", "elapsed_ms",
    "webhook_url", "flow_name", "flow_uuid",
]
DETAIL_UPDATE_COLS = [
    "fired_at", "request_method", "request_path", "request_host",
    "request_headers", "request_body",
    "response_status_line", "response_headers", "response_body",
]


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def run_ingest(
    session,                     # requests.Session, already carrying the cookie
    list_rows: Iterable[dict],   # from the list-page scraper, newest-first
    base_url: str,
    max_pages: int,
    dry_run: bool = False,
    backfill: bool = False,
) -> None:
    """
    TWO MODES. The distinction is the whole ballgame.

    INCREMENTAL (default, daily):
        Walk the newest-first list and STOP at the first id <= MAX(httplog_id).
        Everything above the watermark is new; everything below is already stored.
        Correct and cheap for the daily run.

    BACKFILL (--backfill):
        The watermark is a CEILING, not a floor. Incremental mode stops at the
        newest stored row, so it can only ever move FORWARD in time -- it can
        never reach the ~55k rows of history sitting BELOW the watermark.
        (Bug found 2026-07-11: a 200-page backfill chunk ingested 27 rows and
        exited in two minutes, because it hit the watermark on page 1 and broke.)

        Backfill therefore IGNORES the watermark and pages all the way down,
        SKIPPING rows already stored rather than stopping at them. Already-stored
        rows cost nothing: we skip the detail HTTP fetch entirely. MERGE makes
        re-encountering them harmless regardless.

        Bounded by --max-pages. Resumable: each chunk re-pages from the top and
        continues into whatever it has not yet fetched. Chunks compose.
    """
    client = bq_client()
    now = dt.datetime.now(dt.timezone.utc)

    if not list_rows:
        print("[ingest] scraper returned no rows — nothing to do")
        return

    if backfill:
        ids = [r["httplog_id"] for r in list_rows]
        lo, hi = min(ids), max(ids)
        stored = get_stored_ids(client, lo, hi)
        new_rows = [r for r in list_rows if r["httplog_id"] not in stored]
        # Oldest first. TextIt deletes the log from its oldest end, so after an
        # outage the rows nearest to being deleted are fetched before the newer
        # ones, which stay at the source for days. Each row is committed with its
        # own detail, so the order changes nothing else.
        new_rows.sort(key=lambda r: r["httplog_id"])
        print(f"[backfill] scraped {len(list_rows)} rows spanning {lo}..{hi}")
        print(f"[backfill] {len(stored)} already stored (skipping detail fetch)")
        print(f"[backfill] {len(new_rows)} to fetch")
    else:
        watermark = get_watermark(client)
        if watermark is None:
            print("[watermark] table empty — first run; ingesting everything available")
            new_rows = list(list_rows)
        else:
            print(f"[watermark] highest stored httplog_id = {watermark}")
            new_rows = []
            for r in list_rows:
                if r["httplog_id"] <= watermark:
                    break  # newest-first: everything past here is already stored
                new_rows.append(r)

    if not new_rows:
        print("[ingest] no new rows — nothing to do")
        return

    print(f"[ingest] {len(new_rows)} new rows to fetch details for")

    log_batch, detail_batch = [], []
    fetch_failures = 0
    committed = 0
    committed_detail = 0
    orphaned: list[int] = []   # list rows whose detail fetch failed

    def flush() -> tuple[int, int]:
        """MERGE whatever is buffered, then clear the buffers. Idempotent.

        Returns (log_rows, detail_rows) SEPARATELY. They are not always equal:
        a failed detail fetch still contributes a list row. Reporting the log
        count for both would misrepresent coverage."""
        nonlocal log_batch, detail_batch
        if not log_batch and not detail_batch:
            return (0, 0)
        try:
            a = merge_rows(client, T_LOG, log_batch, LOG_SCHEMA, LOG_UPDATE_COLS)
            b = merge_rows(client, T_DETAIL, detail_batch, DETAIL_SCHEMA, DETAIL_UPDATE_COLS)
        except GoogleAPIError as exc:
            die(f"BigQuery write failed: {exc}")
        log_batch, detail_batch = [], []
        return (a, b)

    for i, r in enumerate(new_rows, 1):
        hid = r["httplog_id"]

        sc = r.get("status_code")
        log_batch.append({
            "httplog_id": hid,
            "fired_at": r["fired_at"].isoformat(),
            "status_code": sc,
            "is_failure": (sc is None) or not (200 <= sc <= 299),  # NULL = timeout = failure
            "elapsed_ms": r.get("elapsed_ms"),
            "webhook_url": r.get("webhook_url"),
            "flow_name": r.get("flow_name"),
            "flow_uuid": r.get("flow_uuid"),
            "ingested_at": now.isoformat(),
        })

        try:
            resp = session.get(f"{base_url}/httplog/read/{hid}/", timeout=30)
            resp.raise_for_status()
            d = parse_detail(resp.text, hid)
        except DetailParseError as exc:
            die(
                f"Detail parser failed on httplog {hid}.\n{exc}\n\n"
                "TextIt's HTML has probably changed. NOTHING has been written.\n"
                "Fix the parser before re-running."
            )
        except Exception as exc:
            # The list row is ALREADY in log_batch. If we merge it without a
            # detail row, the watermark advances past it and the body is never
            # fetched again -- a permanent, invisible hole. So: DROP the list row
            # too, and let a re-run pick the whole pair up cleanly.
            fetch_failures += 1
            orphaned.append(hid)
            if log_batch and log_batch[-1]["httplog_id"] == hid:
                log_batch.pop()
            if fetch_failures <= 10:
                print(f"  [warn] httplog {hid}: detail fetch failed ({exc}) — "
                      f"row DEFERRED to next run (not partially ingested)")
            elif fetch_failures == 11:
                print(f"  [warn] ... further detail-fetch failures suppressed")
            time.sleep(DETAIL_DELAY_SEC)
            continue

        d["fired_at"] = r["fired_at"].isoformat()
        d["detail_fetched_at"] = now.isoformat()
        detail_batch.append(d)

        if i % 250 == 0:
            print(f"  ... {i}/{len(new_rows)} details fetched")

        # checkpoint: commit progress so a later failure cannot discard it
        if not dry_run and len(log_batch) >= CHECKPOINT_EVERY:
            a, b = flush()
            committed += a
            committed_detail += b
            print(f"  [checkpoint] committed {committed}/{len(new_rows)} rows")

        time.sleep(DETAIL_DELAY_SEC)

    if not log_batch and committed == 0:
        die("Zero rows assembled despite new rows being detected. Refusing to write.")

    if dry_run:
        print(f"\n[DRY RUN] would MERGE {len(log_batch)} rows -> webhook_log")
        print(f"[DRY RUN] would MERGE {len(detail_batch)} rows -> webhook_log_detail")
        if detail_batch:
            print("[DRY RUN] sample masked request_headers:")
            print("   ", detail_batch[0]["request_headers"].replace("\n", " | ")[:160])
        print("[DRY RUN] nothing written.")
        return

    a, b = flush()  # final partial batch
    committed += a
    committed_detail += b
    n1, n2 = committed, committed_detail

    # ---- POST-WRITE INTEGRITY ASSERTION ----
    # The 2026-07-11 duplicate bug survived a green idempotency test and only
    # surfaced because Logan eyeballed COUNT(*) vs COUNT(DISTINCT). A pipeline
    # that silently corrupts its own key is worse than one that fails. Assert
    # the invariant on every single run, and shout if it ever breaks.
    chk = list(client.query(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT httplog_id) AS d FROM `{T_LOG}`
    """).result())[0]
    if chk.n != chk.d:
        print("", file=sys.stderr)
        print(f"*** INTEGRITY FAILURE: webhook_log has {chk.n} rows but only "
              f"{chk.d} distinct httplog_ids ({chk.n - chk.d} duplicates).",
              file=sys.stderr)
        print("    httplog_id is the MERGE key -- duplicates mean the dedupe is broken.",
              file=sys.stderr)
        print("    Run dedupe_webhook_log.sql, then investigate before ingesting again.",
              file=sys.stderr)
        raise IngestAborted(f"INTEGRITY FAILURE: webhook_log has {chk.n} rows but only "
                            f"{chk.d} distinct httplog_ids")

    print(f"\n[ingest] MERGED {n1} rows -> webhook_log")
    print(f"[ingest] MERGED {n2} rows -> webhook_log_detail")
    if fetch_failures:
        print(f"[ingest] {fetch_failures} detail fetch(es) failed — those rows were "
              f"DEFERRED, not partially written. Re-run to pick them up.")
        if orphaned[:20]:
            print(f"[ingest] deferred httplog_ids (first 20): {orphaned[:20]}")

    # webhook_log and webhook_log_detail must stay in lockstep. A list row with no
    # body is a permanent hole once the watermark passes it.
    if n1 != n2:
        print(f"\n*** INTEGRITY FAILURE: merged {n1} list rows but {n2} detail rows.",
              file=sys.stderr)
        print("    These must match. A list row without a body is unrecoverable "
              "once the watermark advances.", file=sys.stderr)
        raise IngestAborted(f"INTEGRITY FAILURE: merged {n1} list rows but {n2} detail rows")
    print("[ingest] done")
