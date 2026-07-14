"""
textit_webhook_failures.py

Pulls TextIt's UI webhook HTTP log (textit.com/httplog/webhooks/) -- the ONLY
source of webhook status codes + elapsed times. The API does not expose it
(TESTED 2026-07-11: the workspace advertises 24 endpoints; http_logs and
webhook_events both 404). So we page the authenticated HTML console and parse it.

AUTH: an authenticated console page, not the API. Needs a logged-in browser
session cookie, which a HUMAN captures (DevTools > Application > Cookies) and
writes to Secret Manager:

    gcloud secrets versions add textit-session-cookie --data-file=-
    # paste:  sessionid=xxxx; csrftoken=yyyy      then Ctrl-D (or Ctrl-Z, Enter on Windows)

Programmatic login is IMPOSSIBLE (AWS WAF -- see get_cookie()). This manual step
cannot be removed; it can only be relocated, which is what Secret Manager does.

Run modes are unchanged from the local version -- --ingest, --backfill, --dry-run,
--days, --flow, --url, --status-class, etc. See parse_args(). In Cloud Run the job
is invoked by main.py, which supplies the incremental-ingest arguments.
"""
from __future__ import annotations
import argparse, csv, os, re, sys, time
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

BASE = os.getenv("TEXTIT_BASE_URL", "https://textit.com").rstrip("/")
LOG_PATH = "/httplog/webhooks/"


def parse_args():
    p = argparse.ArgumentParser(description="Pull + filter TextIt webhook HTTP log")
    p.add_argument("--days", type=int)
    p.add_argument("--since")
    p.add_argument("--flow", action="append", default=[])
    p.add_argument("--flow-uuid", action="append", default=[])
    p.add_argument("--url")
    p.add_argument("--status-class",
                   choices=["non2xx", "4xx", "5xx", "timeout", "all"],
                   default="non2xx")
    p.add_argument("--include-success", action="store_true")
    p.add_argument("--max-pages", type=int, default=1000)
    p.add_argument("--prefix", default="textit_webhook_failures")
    p.add_argument("--verbose", action="store_true")
    # --- BigQuery ingest mode ---
    p.add_argument("--ingest", action="store_true",
                   help="Ingest into BigQuery (RESPONSES.webhook_log + webhook_log_detail) "
                        "instead of writing a CSV. Incremental: stops at the highest "
                        "httplog_id already stored. Idempotent (MERGE).")
    p.add_argument("--backfill", action="store_true",
                   help="With --ingest: page PAST already-stored rows into older history "
                        "instead of stopping at the watermark. Required for the initial "
                        "backfill -- incremental mode is a ceiling and can only move forward. "
                        "Skips the detail fetch for rows already in BQ. Resumable; bounded "
                        "by --max-pages.")
    p.add_argument("--dry-run", action="store_true",
                   help="With --ingest: fetch + parse + mask, print what WOULD be written, "
                        "write NOTHING to BigQuery.")
    return p.parse_args()


def get_cookie() -> str:
    """
    The TextIt session cookie, from Secret Manager.

    WHY SECRET MANAGER AND NOT AN ENV VAR / CREDENTIAL MANAGER:
      1. Windows Credential Manager LOST this credential on 2026-07-13 at ~17:30 --
         not expired, ABSENT. `cmdkey /list` returned `* NONE *` and six consecutive
         hourly runs failed. No audit trail, undiagnosable. Secret Manager does not
         silently drop secrets, and it versions every write.
      2. A Cloud Run env var is PLAINTEXT to anyone with roles/run.viewer via a
         read-only `gcloud run services describe`. This cookie is a live TextIt
         session -- full console access to subscriber PII. It does not go in an env var.

    Reads the LATEST version every run, so refreshing the cookie is a single
    `gcloud secrets versions add` with no redeploy.

    WHY A HUMAN STILL HAS TO REFRESH IT (settled 2026-07-13, do not re-litigate):
      TextIt sits behind AWS WAF bot protection. Programmatic login is IMPOSSIBLE --
      the POST returns 202 with a challenge page requiring JS execution in a real
      browser (probe_textit_login.py / probe_textit_login2.py in kriton-dev/EarlyAlert).
      No header, no cookie, no form field gets around it; that is what it exists to
      prevent. When TextIt invalidates the session, this job fails LOUD (exit 2) and
      Logan re-captures. That is the accepted cost.
    """
    # Local/manual override so the job can be run outside GCP against a pasted cookie.
    env = os.getenv("TEXTIT_COOKIE")
    if env:
        return env.strip()

    project = os.getenv("SECRET_PROJECT", "early-alert-responses")
    name = os.getenv("TEXTIT_COOKIE_SECRET", "textit-session-cookie")
    try:
        from google.cloud import secretmanager
        sm = secretmanager.SecretManagerServiceClient()
        path = f"projects/{project}/secrets/{name}/versions/latest"
        return sm.access_secret_version(name=path).payload.data.decode("utf-8").strip()
    except Exception as exc:
        print(f"Could not read secret '{name}' from Secret Manager: {exc}",
              file=sys.stderr)
        sys.exit(2)


def make_session() -> requests.Session:
    cookie = get_cookie()
    if not cookie:
        print("TextIt session cookie is EMPTY. Re-capture it from the browser and "
              "add a new version:\n"
              "  gcloud secrets versions add textit-session-cookie --data-file=-",
              file=sys.stderr)
        sys.exit(2)
    s = requests.Session()
    s.headers.update({
        "Cookie": cookie.strip(),
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "Mozilla/5.0 (textit-webhook-failures-puller)",
        "X-Requested-With": "XMLHttpRequest",  # httplog returns the table fragment
    })
    return s


class EndOfLog(Exception):
    """TextIt 404s past the last page of the httplog.

    This is the NORMAL terminal condition for a full crawl, not a failure. The
    log is finite (~1,599 pages at ~55k rows), and page N+1 past the end returns
    404 rather than an empty page. Treating it as fatal (the original behavior)
    crashed a 2,400-page backfill at page 1600 and DISCARDED every row collected
    up to that point. Found 2026-07-11."""


def get_page(session: requests.Session, page: int) -> str:
    url = f"{BASE}{LOG_PATH}?page={page}"
    for _ in range(5):
        r = session.get(url, timeout=45)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5") or 5)
            time.sleep(wait); continue
        if r.status_code in (401, 403):
            print(f"HTTP {r.status_code} on page {page} — session cookie is missing "
                  f"or expired. Re-capture TEXTIT_COOKIE from the browser.",
                  file=sys.stderr)
            sys.exit(2)
        if r.status_code == 404:
            raise EndOfLog(page)
        r.raise_for_status()
        return r.text
    raise RuntimeError(f"Repeated 429s on page {page}")


ELAPSED_RE = re.compile(r"([\d,]+)\s*ms")
HTTPLOG_ID_RE = re.compile(r"/httplog/read/(\d+)/")


def httplog_id_from_path(detail_path: str) -> int | None:
    """'/httplog/read/121085568/' -> 121085568. TextIt's own row id; monotonic.
    This is the PK / MERGE key for the whole pipeline."""
    m = HTTPLOG_ID_RE.search(detail_path or "")
    return int(m.group(1)) if m else None


def parse_rows(html: str) -> tuple[list[dict], int | None]:
    """Return (rows, total_count). total_count parsed from 'X - Y of Z' if present."""
    soup = BeautifulSoup(html, "html.parser")

    total = None
    rng = soup.find(class_="paging-range")
    if rng:
        m = re.search(r"of\s+([\d,]+)", rng.get_text())
        if m:
            total = int(m.group(1).replace(",", ""))

    rows = []
    table = soup.find("table", class_="list")
    if not table:
        return rows, total
    body = table.find("tbody")
    if not body:
        return rows, total

    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        # URL + detail link (col 0)
        url_a = tds[0].find("a")
        webhook_url = url_a.get_text(strip=True) if url_a else tds[0].get_text(strip=True)
        detail_href = url_a["href"] if (url_a and url_a.has_attr("href")) else ""
        # Status (col 1) — '--' when timed out (no HTTP response)
        status_txt = tds[1].get_text(strip=True)
        status_code = None
        if status_txt.isdigit():
            status_code = int(status_txt)
        # Elapsed (col 2)
        em = ELAPSED_RE.search(tds[2].get_text())
        elapsed_ms = int(em.group(1).replace(",", "")) if em else None
        # Flow name + editor uuid (col 3)
        flow_a = tds[3].find("a")
        flow_name = tds[3].get_text(strip=True)
        flow_uuid = ""
        if flow_a and flow_a.has_attr("href"):
            fm = re.search(r"/flow/editor/([0-9a-f-]{36})/", flow_a["href"])
            if fm:
                flow_uuid = fm.group(1)
        # Time (col 4) — temba-date value attr is precise UTC ISO
        td = tds[4].find("temba-date")
        ts_iso = td["value"] if (td and td.has_attr("value")) else tds[4].get_text(strip=True)

        rows.append({
            "time_utc": ts_iso,
            "status": status_txt,          # keep raw ('--' preserved)
            "status_code": status_code,    # int or None (None == timeout/no-response)
            "elapsed_ms": elapsed_ms,
            "webhook_url": webhook_url,
            "flow_name": flow_name,
            "flow_uuid": flow_uuid,
            "detail_path": detail_href,    # /httplog/read/<id>/ for drill-in
            "httplog_id": httplog_id_from_path(detail_href),
        })
    return rows, total


def is_failure(row: dict, status_class: str) -> bool:
    code = row["status_code"]
    timed_out = (code is None)  # '--' / no response
    if status_class == "all":
        return True
    if status_class == "timeout":
        return timed_out
    if status_class == "4xx":
        return code is not None and 400 <= code <= 499
    if status_class == "5xx":
        return code is not None and 500 <= code <= 599
    # non2xx (default): timeouts + anything outside 200-299
    if timed_out:
        return True
    return not (200 <= code <= 299)


def parse_iso(s: str) -> datetime:
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def row_time(row: dict) -> datetime | None:
    try:
        return parse_iso(row["time_utc"])
    except Exception:
        return None


def main():
    args = parse_args()
    session = make_session()

    # --ingest captures EVERY webhook fire, not just failures (scope decision
    # 2026-07-11). Without this, the default --status-class non2xx filter would
    # silently ingest failures only, and webhook_log would be useless for
    # success-rate / volume trend — the whole reason for keeping list rows
    # indefinitely. Explicit override beats a surprising default.
    if args.ingest and not args.include_success:
        args.include_success = True
        if args.verbose:
            print("[ingest] capturing ALL rows (success + failure), not just failures",
                  file=sys.stderr)

    cutoff = None
    if args.days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    if args.since:
        since_dt = parse_iso(args.since)
        cutoff = max(cutoff, since_dt) if cutoff else since_dt

    flow_needles = [f.lower() for f in args.flow]
    uuid_set = set(u.lower() for u in args.flow_uuid)
    url_needle = args.url.lower() if args.url else None

    matches: list[dict] = []
    total_rows_scanned = 0
    total_reported = None
    page = 0
    stop = False
    cutoff_reached = False   # True once we page past the date cutoff (full coverage)
    capped_early = False     # True if max-pages stopped us before reaching cutoff

    while not stop:
        page += 1
        if page > args.max_pages:
            if cutoff is not None and not cutoff_reached:
                capped_early = True
            if args.verbose:
                print(f"Hit --max-pages {args.max_pages}; stopping.", file=sys.stderr)
            break
        try:
            html = get_page(session, page)
        except EndOfLog:
            # Reached the true end of TextIt's retained log. Everything available
            # has been scraped; proceed to ingest what we have.
            print(f"[paging] reached end of TextIt log at page {page} "
                  f"({total_rows_scanned} rows scraped)", file=sys.stderr)
            cutoff_reached = True
            break
        rows, total = parse_rows(html)
        if total is not None:
            total_reported = total
        if not rows:
            cutoff_reached = True  # ran out of log => covered everything requested
            break
        total_rows_scanned += len(rows)

        for row in rows:
            rt = row_time(row)
            # date-window stop: log is newest-first, so once a row is older than
            # cutoff, every subsequent row is too — finish this page then stop.
            if cutoff and rt and rt < cutoff:
                stop = True
                cutoff_reached = True
                continue
            # dimension filters
            if uuid_set and row["flow_uuid"].lower() not in uuid_set:
                continue
            if flow_needles and not any(n in row["flow_name"].lower() for n in flow_needles):
                continue
            if url_needle and url_needle not in row["webhook_url"].lower():
                continue
            # status filter (unless including successes)
            if not args.include_success and not is_failure(row, args.status_class):
                continue
            matches.append(row)

        if args.verbose:
            newest = rows[0]["time_utc"] if rows else "?"
            oldest = rows[-1]["time_utc"] if rows else "?"
            print(f"page {page}: {len(rows)} rows [{oldest} .. {newest}], "
                  f"matches so far {len(matches)}", file=sys.stderr)

    # ---------------- BigQuery ingest mode ----------------
    if args.ingest:
        from textit_webhook_ingest import run_ingest

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

        # Structural self-check. The scraper has NO API contract — a TextIt reskin
        # silently yields zero parsed rows. Writing zero rows into a table people
        # trust is worse than failing, especially given TextIt's ~3.85-day retention
        # (a gap noticed on day 5 is unrecoverable). So: fail loud, non-zero.
        if total_rows_scanned > 0 and not ing_rows:
            print("", file=sys.stderr)
            print("*** INGEST ABORTED: scraped %d rows but parsed ZERO usable ones."
                  % total_rows_scanned, file=sys.stderr)
            print("    Every row lacked an httplog_id or a parseable timestamp, which",
                  file=sys.stderr)
            print("    means TextIt's HTML has almost certainly changed. Nothing was",
                  file=sys.stderr)
            print("    written. Fix the parser before re-running.", file=sys.stderr)
            sys.exit(3)

        ing_rows.sort(key=lambda r: r["httplog_id"], reverse=True)  # newest-first
        run_ingest(session, ing_rows, BASE, args.max_pages,
                   dry_run=args.dry_run, backfill=args.backfill)
        return

    # ---------------- CSV mode (default) ----------------
    out = f"{args.prefix}.csv"
    fieldnames = ["time_utc", "status", "status_code", "elapsed_ms",
                  "webhook_url", "flow_name", "flow_uuid", "detail_path"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(matches)

    print(f"Pages fetched: {page}")
    print(f"Rows scanned: {total_rows_scanned}"
          + (f" (log reports {total_reported} total)" if total_reported else ""))
    print(f"Matches: {len(matches)}")
    print(f"CSV: {out}")
    if capped_early:
        print("")
        print("*** WARNING: --max-pages stopped the scan BEFORE reaching the requested")
        print(f"    time window. Only the most recent {total_rows_scanned} rows were")
        print("    checked; older failures in the window were NOT seen. This result is")
        print("    PARTIAL — do not treat 'Matches' as complete. Re-run with a higher")
        print(f"    --max-pages (this log runs ~15k rows/day, ~600 pages).")
    elif not args.include_success and len(matches) == 0:
        cov = "the scanned window" if cutoff is None else "the requested time window"
        print(f"=> No failures matched the given filters in {cov} (coverage complete).")


if __name__ == "__main__":
    main()
