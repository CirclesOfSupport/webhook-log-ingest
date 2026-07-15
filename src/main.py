import os
import json
import logging
import traceback

from flask import Flask, request, jsonify

from textit_webhook_failures import make_session, get_page, parse_rows, row_time, EndOfLog, BASE
from textit_webhook_ingest import run_ingest

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

    try:
        session = make_session()

        # Page newest-first, collecting every fire (success + failure).
        # webhook_log is the volume/success-rate record, not just a failure log.
        matches = []
        scanned = 0
        page = 0
        while page < MAX_PAGES:
            page += 1
            try:
                html = get_page(session, page)
            except EndOfLog:
                # 404 past the last page is the NORMAL end of the log (~1,600
                # pages), not an error.
                break
            rows, _total = parse_rows(html)
            if not rows:
                break
            scanned += len(rows)
            matches.extend(rows)

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

        ing_rows.sort(key=lambda x: x["httplog_id"], reverse=True)

        # run_ingest carries the load-bearing behavior unchanged: incremental
        # watermark, source-batch dedupe before MERGE, DEFER-the-pair on a failed
        # detail fetch, 500-row checkpoints, and the COUNT(*)==COUNT(DISTINCT)
        # integrity assertion. It raises on any of them.
        run_ingest(session, ing_rows, BASE, MAX_PAGES, dry_run=False, backfill=False)

    except Exception as e:
        tb = traceback.format_exc()
        reason = classify(str(e) + "\n" + tb)
        emit_failure(reason, str(e) + "\n" + tb)
        return jsonify({"status": "error", "reason": reason, "message": str(e)}), 500

    return jsonify({
        "status": "success",
        "pages": page,
        "rows_scanned": scanned,
        "ingested": len(ing_rows),
    }), 200


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
