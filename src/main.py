import os
import json
import logging
import traceback

from flask import Flask, request, jsonify

from textit_webhook_failures import make_session, get_page, parse_rows, row_time, EndOfLog, CookieError, BASE
from textit_webhook_ingest import run_ingest, get_watermark, bq_client

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Auth is IAM (Cloud Scheduler mints an OIDC token; --no-allow-unauthenticated
# gates the door). SYNC_PASSWORD is an optional SECOND layer, same as the other
# services -- if set in the console it is enforced, if unset it is skipped. The
# only caller is Cloud Scheduler, which holds a Google identity, so IAM alone is
# sufficient and the password can stay unset.
SYNC_PASSWORD = os.environ.get("SYNC_PASSWORD", "")

# Safety ceiling on paging for the incremental run. Incremental mode stops at the
# stored watermark anyway, so this is a runaway-crawl guard, not a filter. At
# ~15k rows/day and 25 rows/page an hour of traffic is ~25 pages; 120 gives ~5h
# of slack for a missed run without ever letting the crawl hammer TextIt's UI.
MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))

# Incremental safety ceiling. Incremental now pages DOWN until it reaches the
# stored watermark (proving a contiguous fetch -- no interior hole), rather than
# stopping at a fixed page count and possibly leaving a gap between what it
# scraped and the watermark. But an unbounded loop could run past the 3600s
# request timeout after a long outage, so we cap it: if the scrape has not
# reached the watermark within this many pages, stop and leave a CONTIGUOUS
# TRAILING EDGE (safely resumable next run), never an interior hole. A normal
# hour is a few hundred rows (~a dozen pages), nowhere near this.
INCREMENTAL_CEILING = int(os.environ.get("INCREMENTAL_CEILING", "1500"))


def emit_failure(reason: str, detail: str) -> None:
    """
    One structured JSON line at ERROR severity. Cloud Logging lifts a bare JSON
    object into jsonPayload, so a log-based alert policy can filter on
    jsonPayload.event="ingest_failed" and put jsonPayload.reason in the email
    subject. This is the ONLY hook the (later, separate) email alert needs from
    this code -- nothing here changes when that policy is added.
    """
    logger.error(json.dumps({
        "event": "ingest_failed",
        "reason": reason,
        "detail": detail[-4000:],
        "severity": "ERROR",
    }))


def classify(text: str) -> str:
    """Name the cause so the alert email can name it too."""
    t = text or ""
    if "BigQuery auth failed" in t or "Reauthentication is needed" in t:
        return "bq_auth"
    if ("session cookie is missing or expired" in t
            or "Could not read secret" in t
            or "session cookie is EMPTY" in t
            or "HTTP 403" in t or "HTTP 401" in t):
        return "cookie_expired"
    if "parsed ZERO usable" in t or "DetailParseError" in t or "parser is stale" in t:
        return "parser_stale"
    if "INTEGRITY FAILURE" in t:
        return "integrity"
    return "unknown"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/run", methods=["POST"])
def run():
    """
    Page TextIt's webhook HTTP log and MERGE new rows into
    RESPONSES.webhook_log (+ webhook_log_detail). Called hourly by Cloud
    Scheduler, or on-demand.

    Incremental: walks the newest-first list and stops at the stored
    MAX(httplog_id) watermark. Idempotent -- MERGE on httplog_id, so a double
    run cannot duplicate a row. On any failure it emits a structured
    ingest_failed log line (see emit_failure) and returns 500.

    Request body (JSON), optional:
        {"password": "<SYNC_PASSWORD>"}   # only checked if SYNC_PASSWORD is set

    Response:
        {"status": "success", "pages": 3, "rows_scanned": 74, "ingested": <n>}
    """
    body = request.get_json(force=True, silent=True) or {}
    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    # Backfill mode (one-time gap repair, not the hourly path). Incremental stops
    # at the watermark and so CANNOT fill an interior hole -- e.g. the 21k-row gap
    # left when a killed catch-up run set the watermark near the top. Backfill
    # ignores the watermark and pages DOWN, skipping already-stored rows (no
    # wasted detail fetches), so it fills holes. Resumable: each call re-pages
    # from the top and continues into whatever is still missing; chunks compose.
    #
    #   POST {"backfill": true, "max_pages": 400}
    #
    # max_pages override lets a backfill page far deeper than the hourly ceiling
    # (120) without raising the steady-state ceiling. Bounded per call so one
    # request still finishes inside the 3600s timeout; run repeatedly until the
    # gap query shows the hole closed.
    backfill = bool(body.get("backfill", False))

    # Page ceiling:
    #   backfill    -> caller's max_pages (deep, bounded per call)
    #   incremental -> INCREMENTAL_CEILING (page until we REACH the watermark;
    #                  the ceiling is only a runtime seatbelt, not the normal stop)
    if backfill:
        max_pages = int(body.get("max_pages", MAX_PAGES))
    else:
        max_pages = int(body.get("max_pages", INCREMENTAL_CEILING))

    try:
        session = make_session()

        # In incremental mode we must page DOWN until the scrape reaches the
        # stored watermark. Stopping at a fixed page count while more-than-that
        # new rows exist is EXACTLY what punched the interior holes (2026-07-15,
        # -16): the newest N were ingested, the watermark jumped to the top, and
        # the rows between the old watermark and the oldest scraped page were
        # orphaned forever (incremental never looks below the watermark again).
        #
        # So: fetch the watermark up front, and let the loop stop as soon as a
        # scraped page contains an id <= watermark (proof the fetch is contiguous
        # back to stored data). If we hit the ceiling first, we stop with a
        # CONTIGUOUS TRAILING EDGE -- the next run resumes from the new (higher
        # but still contiguous) watermark. Never an interior hole.
        reached_watermark = False
        if not backfill:
            try:
                watermark = get_watermark(bq_client())
            except Exception:
                watermark = None  # empty table / first run -> page to ceiling
        else:
            watermark = None

        # Page newest-first, collecting every fire (success + failure).
        # webhook_log is the volume/success-rate record, not just a failure log.
        matches = []
        scanned = 0
        page = 0
        while page < max_pages:
            page += 1
            try:
                html = get_page(session, page)
            except EndOfLog:
                # 404 past the last page is the NORMAL end of the log (~1,600
                # pages), not an error.
                break
            rows, _total = parse_rows(html)
            if rows and not backfill and watermark is not None:
                # Reached stored data? Then this scrape is contiguous to the
                # watermark and we can stop after including this page.
                if min(r.get("httplog_id") or 0 for r in rows) <= watermark:
                    reached_watermark = True
            if not rows:
                # PAGE-1 ZERO-ROWS IS SUSPICIOUS, not a quiet day.
                #
                # get_page already catches the recognizable login page (200 +
                # password field). This catches the residual case: a 200 that is
                # neither the log table nor a page we recognize as login -- a
                # generic redirect, an error shell, a reskin. An active pipeline
                # ALWAYS has recent fires, so zero parseable rows on the very
                # first page means we are not looking at the real log. Fail loud
                # rather than returning an empty success and letting the table go
                # stale. Page 2+ returning empty is the normal end of new rows.
                if page == 1:
                    msg = ("page 1 returned zero parseable rows -- not the webhook "
                           "log (bad cookie, redirect, or changed HTML). Refusing "
                           "to report success on an empty read.")
                    emit_failure("cookie_expired", msg)
                    return jsonify({"status": "error", "reason": "cookie_expired",
                                    "message": msg}), 500
                break
            scanned += len(rows)
            matches.extend(rows)
            if reached_watermark:
                break

        # Build the ingest rows. Drop only rows missing an id or a timestamp.
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

        # STRUCTURAL SELF-CHECK. The scraper has no API contract; a TextIt reskin
        # yields zero parseable rows. Writing zero rows into a table people trust
        # is WORSE than failing -- under TextIt's ~3.85-day retention a silent gap
        # noticed days later is unrecoverable. So a stale parser fails loud.
        if scanned > 0 and not ing_rows:
            msg = f"scraped {scanned} rows but parsed ZERO usable -- TextIt HTML likely changed"
            emit_failure("parser_stale", msg)
            return jsonify({"status": "error", "reason": "parser_stale", "message": msg}), 500

        # If incremental exhausted the ceiling without reaching the watermark,
        # a large backlog remains. This is SAFE (contiguous trailing edge, resumes
        # next run) but worth surfacing so a persistent backlog is visible.
        if (not backfill) and watermark is not None and not reached_watermark \
                and page >= max_pages:
            logger.warning(json.dumps({
                "event": "incremental_ceiling_hit",
                "detail": (f"paged {page} pages without reaching watermark "
                           f"{watermark}; backlog remains, will continue next run"),
                "severity": "WARNING",
            }))

        ing_rows.sort(key=lambda x: x["httplog_id"], reverse=True)

        # run_ingest carries the load-bearing behavior unchanged: incremental
        # watermark, source-batch dedupe before MERGE, DEFER-the-pair on a failed
        # detail fetch, 500-row checkpoints, and the COUNT(*)==COUNT(DISTINCT)
        # integrity assertion. It raises on any of them.
        run_ingest(session, ing_rows, BASE, max_pages, dry_run=False, backfill=backfill)

    except CookieError as e:
        # Typed, so it is labelled cookie_expired regardless of the message text
        # (the login-page message contains no keyword classify() would match).
        emit_failure("cookie_expired", str(e))
        return jsonify({"status": "error", "reason": "cookie_expired",
                        "message": str(e)}), 500

    except Exception as e:
        tb = traceback.format_exc()
        reason = classify(str(e) + "\n" + tb)
        emit_failure(reason, str(e) + "\n" + tb)
        return jsonify({"status": "error", "reason": reason, "message": str(e)}), 500

    return jsonify({
        "status": "success",
        "mode": "backfill" if backfill else "incremental",
        "pages": page,
        "rows_scanned": scanned,
    }), 200


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
