"""Detail-page parser against both known TextIt layouts (synthetic pages, no real data)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from textit_webhook_detail import parse_detail, status_code_from_status_line  # noqa: E402

REQ = ("POST /write HTTP/1.1\nHost: example.run.app\nUser-Agent: test\nContent-Type: application/json\n\n"
       '{"password": "pw", "Comment": "said \\"none yet\\""}')
RESP = 'HTTP/2.0 400 Bad Request\nContent-Type: application/json\n\n{"message": "Invalid JSON"}'


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def old_page(resp=RESP):
    return (f'<html><body><pre class="prettyprint request">{esc(REQ)}</pre>'
            f'<pre class="prettyprint response">{esc(resp)}</pre></body></html>')


def highlight(t):
    first, _, rest = t.partition("\n")
    head, _, body = rest.partition("\n\n")
    lines = "".join(f'<span class="hl-key">{esc(h.split(":")[0])}</span>{esc(h[len(h.split(":")[0]):])}\n'
                    for h in head.split("\n"))
    return f'<span class="hl-method">{esc(first)}</span>\n{lines}\n{esc(body)}'


def new_page(resp=RESP, with_response=True):
    resp_card = (f'<div class="hlog-card"><div class="hlog-bar"><span>Response</span>'
                 f'<span class="hlog-meta"><span>798 ms</span><span class="hlog-status">400</span></span></div>'
                 f'<pre>{highlight(resp) if resp.startswith("HTTP/") else esc(resp)}</pre></div>') if with_response else ""
    return (f'<html><body><div class="hlog"><div class="hlog-card"><div class="hlog-bar"><span>Request</span>'
            f'<span class="hlog-url">https://example.run.app/write</span></div><pre>{highlight(REQ)}</pre></div>'
            f'{resp_card}</div></body></html>')


def test_both_layouts_parse_the_same():
    a = parse_detail(old_page(), 1)
    b = parse_detail(new_page(), 1)
    assert a == b
    assert b["request_method"] == "POST" and b["request_path"] == "/write"
    assert b["request_host"] == "example.run.app"
    assert status_code_from_status_line(b["response_status_line"]) == 400
    assert '"pw"' not in b["request_body"]          # password masked
    assert 'none yet' in b["request_body"]


def test_new_layout_timeout():
    b = parse_detail(new_page(resp="Connection Error"), 2)
    assert b["response_status_line"] == "Connection Error"
    assert status_code_from_status_line(b["response_status_line"]) is None


def test_new_layout_no_response_card():
    b = parse_detail(new_page(with_response=False), 3)
    assert b["response_status_line"] == ""


def test_unknown_layout_still_fails_loud():
    import pytest
    from textit_webhook_detail import DetailParseError
    with pytest.raises(DetailParseError):
        parse_detail("<html><body><div>nothing</div></body></html>", 4)


def test_die_raises_not_exits():
    import pytest
    import textit_webhook_ingest as ing
    with pytest.raises(ing.IngestAborted):
        ing.die("parser is stale")
    assert not issubclass(ing.IngestAborted, SystemExit)
