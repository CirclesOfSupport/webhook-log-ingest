#!/usr/bin/env python3
"""
textit_webhook_detail.py
========================
Parser + credential-masking for TextIt httplog DETAIL pages
(/httplog/read/<id>/), plus the BigQuery ingest for the webhook_log pipeline.

Structure of the detail page is TESTED (2026-07-11, six pages: one 200 plus all
five failure shapes). See early_alert/handoffs/open/2026-07-11_webhook_log_pipeline.md

Two payload containers, both plain server-rendered HTML:
    <pre class="prettyprint request">   raw HTTP request
    <pre class="prettyprint response">  raw HTTP response
Each is:  <first line> \n <headers...> \n <blank> \n <body>

*** CRITICAL — TIMEOUTS RENDER DIFFERENTLY (TESTED, id 121092906) ***
For a timeout the response <pre> is PRESENT but contains only the literal
string "Connection Error" — no status line, no headers, no blank separator,
no body. Parsing from a 200-only sample crashes or silently mis-parses every
timeout. handle_response() covers this explicitly.

*** CREDENTIALS APPEAR IN TWO PLACES — BOTH ARE MASKED ***
  1. Request HEADERS:  "Token: <SHARED_SECRET>"    (get-responses_v2 calls)
  2. Request BODIES:   {"password": "<PASSWORD>"}  (sheet-service /write,
                        Apps Script) — present on essentially every such call.
Header-only masking is INSUFFICIENT. Masking happens at ingest, before the
MERGE, and is irreversible.

NOTE — THIS REPO IS PUBLIC. The placeholders above are placeholders. The real
values do NOT go in this file, not even as a docstring example. A credential in
a docstring is one of the four ways sheet-service leaked its password to the
world; the private version of this file carried a live service token here.
"""
from __future__ import annotations

import re
import json
import html

# ---------------------------------------------------------------------------
# Credential masking
# ---------------------------------------------------------------------------

REDACTED = "[REDACTED]"

# Header names to mask (case-insensitive, exact name match on the part before ':')
CREDENTIAL_HEADERS = {
    "token",
    "authorization",
    "password",
    "x-api-key",
    "apikey",
    "api-key",
    "x-auth-token",
}

# Body JSON keys to mask (case-insensitive)
CREDENTIAL_BODY_KEYS = {
    "password",
    "token",
    "secret",
    "apikey",
    "api_key",
    "authorization",
}

# Regex fallback for bodies that will not JSON-parse.
# A malformed body is EXACTLY the kind worth keeping (cf. the "Invalid JSON" 400),
# so we cannot simply skip masking when json.loads fails. This operates on raw text.
_BODY_CRED_RE = re.compile(
    r'("(?:%s)"\s*:\s*)"(?:[^"\\]|\\.)*"' % "|".join(CREDENTIAL_BODY_KEYS),
    re.IGNORECASE,
)


def mask_headers(header_lines: list[str]) -> list[str]:
    """Mask credential headers. All other headers retained verbatim."""
    out = []
    for line in header_lines:
        if ":" in line:
            name, _, _value = line.partition(":")
            if name.strip().lower() in CREDENTIAL_HEADERS:
                out.append(f"{name}: {REDACTED}")
                continue
        out.append(line)
    return out


def mask_body(body: str) -> str:
    """
    Mask credential KEY VALUES inside a body. Everything else stays byte-for-byte.

    Strategy:
      1. Try to parse as JSON and mask structurally (precise; preserves shape).
      2. If that fails (malformed body — the diagnostically interesting case),
         fall back to a regex mask on the raw text. Fails safe.
    """
    if not body or not body.strip():
        return body

    # --- 1. structural (preferred) ---
    try:
        parsed = json.loads(body)
    except Exception:
        parsed = None

    if parsed is not None:
        def _walk(node):
            if isinstance(node, dict):
                return {
                    k: (REDACTED if k.lower() in CREDENTIAL_BODY_KEYS else _walk(v))
                    for k, v in node.items()
                }
            if isinstance(node, list):
                return [_walk(v) for v in node]
            return node

        try:
            return json.dumps(_walk(parsed), indent=2)
        except Exception:
            pass  # fall through to regex

    # --- 2. regex fallback (malformed bodies) ---
    return _BODY_CRED_RE.sub(lambda m: f'{m.group(1)}"{REDACTED}"', body)


# ---------------------------------------------------------------------------
# Detail page parsing
# ---------------------------------------------------------------------------

class DetailParseError(Exception):
    """Raised when the page does not match the expected structure.

    This is DELIBERATELY loud. The scraper has no API contract — a TextIt reskin
    silently changes the HTML. For an ad-hoc CSV tool an empty result was merely
    annoying; for a pipeline two people depend on, a silent empty ingest into a
    trusted table is the WORST available failure mode. Fail non-zero instead.
    """


def _split_http_block(text: str) -> tuple[str, list[str], str]:
    """
    Split a raw HTTP block into (first_line, header_lines, body).

    Shape:   <first line>\n<header>\n<header>\n\n<body...>
    The body may itself contain blank lines, so we split on the FIRST blank only.
    """
    lines = text.splitlines()
    if not lines:
        return "", [], ""

    first_line = lines[0]

    blank_idx = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "":
            blank_idx = i
            break

    if blank_idx is None:
        # Headers only, no body.
        return first_line, lines[1:], ""

    headers = lines[1:blank_idx]
    body = "\n".join(lines[blank_idx + 1:])
    return first_line, headers, body


def parse_detail(page_html: str, httplog_id: int) -> dict:
    """
    Parse one /httplog/read/<id>/ page into the webhook_log_detail row shape.
    Credentials are masked here, before the value ever leaves this function.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(page_html, "html.parser")

    req_pre = soup.find("pre", class_="request")
    resp_pre = soup.find("pre", class_="response")

    # --- structural self-check: loud, not silent ---
    if req_pre is None:
        raise DetailParseError(
            f"httplog {httplog_id}: no <pre class='request'> found. "
            "TextIt's httplog HTML has likely changed — the parser is stale. "
            "Do NOT ingest; fix the parser."
        )

    # get_text() unescapes &quot; -> " automatically
    req_text = req_pre.get_text()
    req_line, req_headers, req_body = _split_http_block(req_text)

    # request line: "POST /get-responses_v2/v2/add HTTP/1.1"
    method = path = None
    m = re.match(r"^(\S+)\s+(\S+)\s+HTTP/", req_line)
    if m:
        method, path = m.group(1), m.group(2)
    else:
        raise DetailParseError(
            f"httplog {httplog_id}: request line did not match "
            f"'<METHOD> <PATH> HTTP/<ver>': {req_line[:120]!r}"
        )

    host = None
    for h in req_headers:
        if h.split(":")[0].strip().lower() == "host":
            host = h.partition(":")[2].strip()
            break

    # --- response: the timeout case is the dangerous one ---
    status_line = ""
    resp_headers: list[str] = []
    resp_body = ""

    if resp_pre is not None:
        resp_text = resp_pre.get_text().strip()

        if not resp_text:
            status_line = ""
        elif not resp_text.startswith("HTTP/"):
            # TIMEOUT. TESTED: contains literally "Connection Error" and nothing else.
            # No status line, no headers, no blank separator, no body.
            status_line = resp_text.splitlines()[0].strip()
            resp_headers = []
            resp_body = ""
        else:
            status_line, resp_headers, resp_body = _split_http_block(resp_pre.get_text())

    return {
        "httplog_id": httplog_id,
        "request_method": method,
        "request_path": path,
        "request_host": host,
        "request_headers": "\n".join(mask_headers(req_headers)),
        "request_body": mask_body(req_body),
        "response_status_line": status_line,
        "response_headers": "\n".join(mask_headers(resp_headers)),
        "response_body": mask_body(resp_body),
    }


def status_code_from_status_line(status_line: str) -> int | None:
    """
    'HTTP/2.0 404 Not Found' -> 404
    'Connection Error'       -> None   (timeout: no HTTP response at all)
    ''                       -> None
    """
    if not status_line or not status_line.startswith("HTTP/"):
        return None
    m = re.match(r"^HTTP/[\d.]+\s+(\d{3})", status_line)
    return int(m.group(1)) if m else None
