"""Backfill fetches the missing rows oldest first (synthetic rows, no network, no BigQuery)."""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import textit_webhook_ingest as ing  # noqa: E402


class FakeResp:
    text = "<html></html>"

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self):
        self.fetched = []

    def get(self, url, timeout=30):
        self.fetched.append(int(url.rstrip("/").split("/")[-1]))
        return FakeResp()


class FakeRow:
    def __init__(self, n, d):
        self.n, self.d = n, d


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class FakeClient:
    def __init__(self):
        self.merged = []

    def query(self, sql):
        n = len(self.merged)
        return FakeQuery([FakeRow(n, n)])


def rows(ids):
    t = dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)
    return [{"httplog_id": i, "fired_at": t, "status_code": 200} for i in ids]


def run(monkeypatch, list_ids, stored, backfill):
    client = FakeClient()
    session = FakeSession()
    monkeypatch.setattr(ing, "bq_client", lambda: client)
    monkeypatch.setattr(ing, "get_stored_ids", lambda c, lo, hi: set(stored))
    monkeypatch.setattr(ing, "get_watermark", lambda c: min(stored) if stored else None)
    monkeypatch.setattr(ing, "parse_detail", lambda html, hid: {"httplog_id": hid})
    monkeypatch.setattr(ing, "DETAIL_DELAY_SEC", 0)

    def merge(c, table, batch, schema, cols):
        if table == ing.T_LOG:
            client.merged.extend(r["httplog_id"] for r in batch)
        return len(batch)

    monkeypatch.setattr(ing, "merge_rows", merge)
    ing.run_ingest(session, rows(list_ids), "https://example", 10, backfill=backfill)
    return session.fetched, client.merged


def test_backfill_fetches_oldest_first(monkeypatch):
    fetched, merged = run(monkeypatch, [109, 108, 107, 106, 105, 104], {108, 105}, backfill=True)
    assert fetched == [104, 106, 107, 109]
    assert sorted(merged) == [104, 106, 107, 109]


def test_incremental_still_newest_first(monkeypatch):
    fetched, _ = run(monkeypatch, [109, 108, 107, 106, 105], {105}, backfill=False)
    assert fetched == [109, 108, 107, 106]
