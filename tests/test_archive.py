"""Tests for raw-byte batch archiving and verified downloads."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import archive
from app.db import connect as db_connect
from app.db import init_db
from app.logic import ingest, parse_csv

BASE = 1793491200  # 2026-11-01T00:00:00Z, hour-aligned
H = 3600


def hour(n: int) -> int:
    return BASE + n * H


def csv_bytes(rows: list[tuple[str, int, float]], newline: str = "\n") -> bytes:
    lines = ["meter_id,utc_second,kwh"]
    lines += [f"{m},{t},{k}" for m, t, k in rows]
    return newline.join(lines).encode("utf-8") + newline.encode("utf-8")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("ARCHIVE_DIR", str(tmp_path / "archives"))
    from app.main import app
    with TestClient(app) as c:
        yield c


def post(client: TestClient, data: bytes):
    return client.post(
        "/batches", files={"file": ("batch.csv", data, "text/csv")})


def archive_files() -> list[str]:
    directory = archive.archive_dir()
    return sorted(os.listdir(directory)) if os.path.isdir(directory) else []


# ---------- happy path: archive + verified download ----------

def test_uploaded_bytes_archived_and_downloaded_verbatim(client):
    raw = csv_bytes([("m1", hour(0), 100.0), ("m1", hour(1), 101.5)], newline="\r\n")
    resp = post(client, raw)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["archive_size"] == len(raw)
    assert body["archive_sha256"] == hashlib.sha256(raw).hexdigest()

    # Exactly one canonical file, no temp leftovers.
    files = archive_files()
    assert files == [f"{body['batch_id']}.csv"]

    dl = client.get(f"/batches/{body['batch_id']}/download")
    assert dl.status_code == 200
    assert dl.content == raw  # byte-identical, CRLF preserved
    assert dl.headers["x-content-sha256"] == body["archive_sha256"]
    assert dl.headers["content-type"].startswith("text/csv")
    assert body["batch_id"] in dl.headers["content-disposition"]


def test_utf8_bom_is_part_of_the_archived_original(client):
    raw = b"\xef\xbb\xbf" + csv_bytes([("m1", hour(0), 1.0)])
    body = post(client, raw).json()
    dl = client.get(f"/batches/{body['batch_id']}/download")
    assert dl.status_code == 200
    assert dl.content == raw
    assert dl.content.startswith(b"\xef\xbb\xbf")


def test_each_accepted_batch_including_retransmit_has_its_own_archive(client):
    raw1 = csv_bytes([("m1", hour(0), 100.0), ("m1", hour(1), 101.0)])
    raw2 = csv_bytes([("m1", hour(0), 100.0), ("m1", hour(1), 101.0)])
    b1 = post(client, raw1).json()
    b2 = post(client, raw2).json()
    assert b1["batch_id"] != b2["batch_id"]
    assert b2["rows_inserted"] == 0 and b2["rows_deduplicated"] == 2

    files = archive_files()
    assert files == sorted([f"{b1['batch_id']}.csv", f"{b2['batch_id']}.csv"])
    for bid, raw in ((b1["batch_id"], raw1), (b2["batch_id"], raw2)):
        dl = client.get(f"/batches/{bid}/download")
        assert dl.status_code == 200
        assert dl.content == raw
        assert hashlib.sha256(dl.content).hexdigest() == dl.headers["x-content-sha256"]


def test_batches_list_exposes_archive_metadata(client):
    body = post(client, csv_bytes([("m1", hour(0), 1.0)])).json()
    listed = client.get("/batches").json()
    assert len(listed) == 1
    item = listed[0]
    assert item["batch_id"] == body["batch_id"]
    assert item["archive_sha256"] == body["archive_sha256"]
    assert item["archive_size"] == body["archive_size"]
    assert item["downloadable"] is True


# ---------- rejected batches must not be accessible ----------

def test_conflict_batch_leaves_no_archive_and_is_not_listed(client):
    post(client, csv_bytes([("m1", hour(0), 100.0), ("m1", hour(1), 101.0)]))
    files_before = archive_files()
    resp = post(client, csv_bytes([("m1", hour(1), 999.0), ("m1", hour(2), 105.0)]))
    assert resp.status_code == 409
    assert archive_files() == files_before  # rejected batch added nothing
    assert len(client.get("/batches").json()) == 1
    assert not any(f.startswith(".tmp-") for f in archive_files())


def test_invalid_csv_never_creates_any_archive_file(client):
    resp = post(client, b"meter_id,utc_second,kwh\nm1,not-a-number,1.0\n")
    assert resp.status_code == 422
    assert archive_files() == []
    assert client.get("/batches").json() == []


# ---------- download error semantics ----------

def test_unknown_and_malformed_batch_ids_return_404(client):
    assert client.get("/batches/does-not-exist/download").status_code == 404
    # Path traversal attempts must never escape the archive directory.
    resp = client.get("/batches/..%2f..%2fetc%2fpasswd/download")
    assert resp.status_code == 404
    assert not any(f.startswith(".tmp-") for f in archive_files())


def test_legacy_batch_without_original_is_explicitly_not_downloadable(client, tmp_path):
    # Simulate a row written by a pre-archiving release.
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO batches (batch_id, received_at, rows_total, rows_inserted,"
            " rows_deduplicated, archive_sha256, archive_size) "
            "VALUES ('aabbccddaabbccddaabbccddaabbccdd', '2026-10-01T00:00:00Z', 1, 1, 0, NULL, NULL)")
    finally:
        conn.close()

    listed = client.get("/batches").json()
    assert listed[0]["downloadable"] is False
    assert listed[0]["archive_sha256"] is None

    resp = client.get("/batches/aabbccddaabbccddaabbccddaabbccdd/download")
    assert resp.status_code == 410
    assert "not available" in resp.json()["detail"]


def test_tampered_archive_is_rejected_without_partial_content(client):
    raw = csv_bytes([("m1", hour(0), 1.0), ("m1", hour(1), 2.0)])
    body = post(client, raw).json()
    path = archive.canonical_path(body["batch_id"])
    # Same length so the size check passes and the SHA-256 mismatch triggers.
    tampered = b"x" * len(raw)
    assert len(tampered) == len(raw)
    with open(path, "wb") as fh:
        fh.write(tampered)
    resp = client.get(f"/batches/{body['batch_id']}/download")
    assert resp.status_code == 500
    assert "SHA-256" in resp.json()["detail"]
    assert b"meter_id" not in resp.content  # no partial CSV bytes leaked


def test_truncated_archive_is_rejected_via_size_check(client):
    body = post(client, csv_bytes([("m1", hour(n), float(n)) for n in range(4)])).json()
    path = archive.canonical_path(body["batch_id"])
    with open(path, "rb") as fh:
        truncated = fh.read()[:10]
    with open(path, "wb") as fh:
        fh.write(truncated)
    resp = client.get(f"/batches/{body['batch_id']}/download")
    assert resp.status_code == 500
    assert "corrupt" in resp.json()["detail"]


def test_missing_archive_file_is_reported_as_integrity_failure(client):
    body = post(client, csv_bytes([("m1", hour(0), 1.0)])).json()
    os.unlink(archive.canonical_path(body["batch_id"]))
    resp = client.get(f"/batches/{body['batch_id']}/download")
    assert resp.status_code == 500
    assert "missing" in resp.json()["detail"]


# ---------- atomicity: failures and interrupted writes ----------

def test_commit_failure_leaves_no_batch_row_and_no_archive(client, monkeypatch):
    import app.logic as logic

    real_connect = logic.connect

    class FailingConnection:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args):
            if sql == "COMMIT":
                raise sqlite3.OperationalError("simulated disk failure on commit")
            return self._real.execute(sql, *args)

        def close(self):
            return self._real.close()

    monkeypatch.setattr(logic, "connect",
                        lambda: FailingConnection(real_connect()))

    with pytest.raises(sqlite3.OperationalError):
        ingest(parse_csv(csv_bytes([("m1", hour(0), 1.0)]).decode()),
               csv_bytes([("m1", hour(0), 1.0)]))

    conn = db_connect()
    try:
        assert conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"] == 0
        assert conn.execute("SELECT COUNT(*) AS n FROM readings").fetchone()["n"] == 0
    finally:
        conn.close()
    assert archive_files() == []


def test_archive_write_failure_leaves_nothing_behind(client, monkeypatch):
    def boom(*_args, **_kwargs):
        raise OSError("simulated archive disk failure")

    monkeypatch.setattr(archive, "store_original", boom)
    # A separate client converts the server-side OSError into an HTTP 500
    # instead of re-raising it.
    from app.main import app
    with TestClient(app, raise_server_exceptions=False) as quiet:
        resp = post(quiet, csv_bytes([("m1", hour(0), 1.0)]))
    assert resp.status_code == 500
    assert archive_files() == []
    conn = db_connect()
    try:
        assert conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"] == 0
    finally:
        conn.close()


def test_startup_sweep_removes_interrupted_ingest_leftovers(client, tmp_path):
    # A normal accepted batch (must survive the sweep).
    body = post(client, csv_bytes([("m1", hour(0), 1.0)])).json()
    good = archive.canonical_path(body["batch_id"])

    # Simulate a process that died between os.replace and COMMIT: canonical
    # file exists but no batch row references it.
    orphan_id = "deadbeefdeadbeefdeadbeefdeadbeef"
    orphan_path = archive.canonical_path(orphan_id)
    with open(orphan_path, "wb") as fh:
        fh.write(b"orphan,never,committed\n")
    # And an interrupted temp write.
    tmp_path = os.path.join(archive.archive_dir(),
                            f".tmp-{orphan_id}-11111111111111111111111111111111")
    with open(tmp_path, "wb") as fh:
        fh.write(b"half-written")

    # Age the interrupted files past the sweep guard.
    for p in (orphan_path, tmp_path):
        old_ts = os.path.getmtime(p) - archive.SWEEP_MIN_AGE_SECONDS - 10
        os.utime(p, (old_ts, old_ts))

    # A brand-new orphan (other instance mid-ingest) must be left alone.
    young_id = "f00df00df00df00df00df00df00df00d"
    young_path = archive.canonical_path(young_id)
    with open(young_path, "wb") as fh:
        fh.write(b"in,flight,now\n")

    from app.main import _sweep_archive_leftovers
    _sweep_archive_leftovers()

    remaining = archive_files()
    assert os.path.basename(good) in remaining
    assert os.path.basename(young_path) in remaining
    assert os.path.basename(orphan_path) not in remaining
    assert os.path.basename(tmp_path) not in remaining


# ---------- concurrency: many writers, every archive verifiable ----------

def test_concurrent_uploads_all_archive_and_verify(client):
    n = 12
    results: list = [None] * n

    def worker(i):
        raw = csv_bytes([("m1", hour(2 * i), float(i)),
                         ("m1", hour(2 * i + 1), float(i) + 0.5)])
        results[i] = (post(client, raw), raw)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r.status_code == 201 for r, _ in results)
    assert len(archive_files()) == n
    assert not any(f.startswith(".tmp-") for f in archive_files())

    for resp, raw in results:
        bid = resp.json()["batch_id"]
        dl = client.get(f"/batches/{bid}/download")
        assert dl.status_code == 200
        assert dl.content == raw
        assert hashlib.sha256(raw).hexdigest() == dl.headers["x-content-sha256"]

    conn = db_connect()
    try:
        assert conn.execute("SELECT COUNT(*) AS n FROM readings").fetchone()["n"] == 2 * n
        assert conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"] == n
    finally:
        conn.close()


# ---------- forward migration of a pre-archiving database ----------

def test_old_schema_database_is_migrated(tmp_path, monkeypatch):
    db_file = tmp_path / "old.db"
    archive_dir_ = tmp_path / "archives"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("ARCHIVE_DIR", str(archive_dir_))

    raw_conn = sqlite3.connect(db_file)
    raw_conn.executescript(
        """
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY, received_at TEXT NOT NULL,
            rows_total INTEGER NOT NULL, rows_inserted INTEGER NOT NULL,
            rows_deduplicated INTEGER NOT NULL);
        CREATE TABLE readings (
            meter_id TEXT NOT NULL, utc_second INTEGER NOT NULL, kwh REAL NOT NULL,
            batch_id TEXT NOT NULL, PRIMARY KEY (meter_id, utc_second));
        CREATE TABLE hourly_usage (
            meter_id TEXT NOT NULL, hour_start INTEGER NOT NULL, kwh_used REAL,
            billable INTEGER NOT NULL, reason TEXT NOT NULL,
            PRIMARY KEY (meter_id, hour_start));
        INSERT INTO batches VALUES ('00112233445566778899aabbccddeeff',
            '2026-10-01T00:00:00Z', 1, 1, 0);
        """)
    raw_conn.commit()
    raw_conn.close()

    init_db()  # must add archive_sha256 / archive_size without losing rows

    from app.main import app
    with TestClient(app) as c:
        legacy = c.get("/batches").json()
        assert legacy[0]["batch_id"] == "00112233445566778899aabbccddeeff"
        assert legacy[0]["downloadable"] is False
        assert c.get("/batches/00112233445566778899aabbccddeeff/download").status_code == 410

        # New uploads on the migrated database work and are archived normally.
        resp = post(c, csv_bytes([("m9", hour(0), 7.0)]))
        assert resp.status_code == 201
        dl = c.get(f"/batches/{resp.json()['batch_id']}/download")
        assert dl.status_code == 200 and dl.content == csv_bytes([("m9", hour(0), 7.0)])
