"""FastAPI surface: batch ingest (CSV upload) plus query endpoints."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, Request, Response, UploadFile
from pydantic import BaseModel

from . import archive
from .db import connect, init_db
from .logic import (HOUR, ConflictError, ValidationError, ingest, parse_csv,
                    utc_iso)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    _sweep_archive_leftovers()
    yield


def _sweep_archive_leftovers() -> None:
    """At startup remove files of ingests interrupted by a process exit.

    Canonical files whose batch row never committed and stale temp files
    are deleted (age-guarded so another instance's in-flight files are
    safe). Sweep failures must not block startup.
    """
    try:
        conn = connect()
        try:
            known = {r["batch_id"] for r in conn.execute("SELECT batch_id FROM batches")}
        finally:
            conn.close()
        archive.sweep_stale(known)
    except Exception:
        pass


app = FastAPI(
    title="Park Energy Backfill API",
    version="1.1.0",
    description=(
        "Receives backfilled cumulative meter readings (CSV), keeps exactly one "
        "copy of identical retransmissions, rejects batches that contradict "
        "stored readings, and derives billable hourly kWh for the billing team. "
        "Every accepted batch is stored as the exact uploaded bytes with a "
        "SHA-256 digest and can be downloaded again for billing audit via "
        "GET /batches/{batch_id}/download. Upload samples via the file picker "
        "below, then query /readings and /hourly-usage."),
    lifespan=lifespan,
)


# ---------- schemas ----------

class IngestResponse(BaseModel):
    batch_id: str
    received_at: str
    rows_total: int
    rows_inserted: int
    rows_deduplicated: int
    meters: list[str]
    hourly_usage_updated: int
    archive_sha256: str
    archive_size: int


class ReadingOut(BaseModel):
    meter_id: str
    utc_second: int
    utc_time: str
    kwh: float
    batch_id: str


class HourlyUsageOut(BaseModel):
    meter_id: str
    hour_start: int
    hour_start_utc: str
    kwh_used: Optional[float]
    billable: bool
    reason: str  # ok | gap | rollback


class BatchOut(BaseModel):
    batch_id: str
    received_at: str
    rows_total: int
    rows_inserted: int
    rows_deduplicated: int
    archive_sha256: Optional[str]
    archive_size: Optional[int]
    downloadable: bool


# ---------- ingest ----------

@app.post("/batches", response_model=IngestResponse, status_code=201,
          summary="Upload a CSV batch of cumulative readings",
          responses={
              409: {"description": "Batch contradicts stored readings; nothing was saved"},
              415: {"description": "Unsupported content type"},
              422: {"description": "CSV failed validation; nothing was saved"},
          })
async def create_batch(request: Request,
                       file: Optional[UploadFile] = File(
                           default=None,
                           description="CSV file with columns meter_id,utc_second,kwh")):
    """Accept the CSV either as a multipart file upload (Swagger file picker)
    or as a raw `text/csv` request body.

    The exact uploaded bytes are archived with their SHA-256 before the
    batch (including all-identical retransmissions) is committed; rejected
    batches leave neither database rows nor an accessible archive.
    """
    raw = await _extract_csv_bytes(request, file)

    try:
        text = raw.decode("utf-8-sig")  # tolerate a BOM
    except UnicodeDecodeError:
        raise HTTPException(status_code=422, detail="CSV must be UTF-8 encoded")

    try:
        rows = parse_csv(text)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        return ingest(rows, raw)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


async def _extract_csv_bytes(request: Request, file: Optional[UploadFile]) -> bytes:
    content_type = request.headers.get("content-type", "")
    if file is not None:
        return await file.read()
    if any(content_type.startswith(t) for t in
           ("text/csv", "text/plain", "application/octet-stream")):
        return await request.body()
    raise HTTPException(
        status_code=415,
        detail="send the CSV as multipart/form-data file upload or as a text/csv body")


# ---------- queries ----------

def _window(from_second: Optional[int], to_second: Optional[int]) -> tuple[int, int]:
    for name, value in (("from_second", from_second), ("to_second", to_second)):
        if value is not None and (value < 0 or value % HOUR != 0):
            raise HTTPException(
                status_code=422,
                detail=f"{name} must be a non-negative UTC hour-aligned second, got {value}")
    lo = from_second if from_second is not None else 0
    hi = to_second if to_second is not None else 2**63 - 1
    if lo > hi:
        raise HTTPException(status_code=422, detail="from_second must be <= to_second")
    return lo, hi


@app.get("/readings", response_model=list[ReadingOut],
         summary="Raw cumulative readings of one meter in a time window")
def get_readings(meter_id: str = Query(..., description="meter identifier"),
                 from_second: Optional[int] = Query(None, description="UTC hour-aligned, inclusive"),
                 to_second: Optional[int] = Query(None, description="UTC hour-aligned, inclusive")):
    lo, hi = _window(from_second, to_second)
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT meter_id, utc_second, kwh, batch_id FROM readings "
            "WHERE meter_id = ? AND utc_second BETWEEN ? AND ? ORDER BY utc_second",
            (meter_id, lo, hi)).fetchall()
    finally:
        conn.close()
    return [ReadingOut(meter_id=r["meter_id"], utc_second=r["utc_second"],
                       utc_time=utc_iso(r["utc_second"]), kwh=r["kwh"],
                       batch_id=r["batch_id"]) for r in rows]


@app.get("/hourly-usage", response_model=list[HourlyUsageOut],
         summary="Hourly kWh usage with billable flag and reason")
def get_hourly_usage(meter_id: str = Query(..., description="meter identifier"),
                     from_second: Optional[int] = Query(None, description="UTC hour-aligned, inclusive"),
                     to_second: Optional[int] = Query(None, description="UTC hour-aligned, inclusive")):
    lo, hi = _window(from_second, to_second)
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT meter_id, hour_start, kwh_used, billable, reason FROM hourly_usage "
            "WHERE meter_id = ? AND hour_start BETWEEN ? AND ? ORDER BY hour_start",
            (meter_id, lo, hi)).fetchall()
    finally:
        conn.close()
    return [HourlyUsageOut(meter_id=r["meter_id"], hour_start=r["hour_start"],
                           hour_start_utc=utc_iso(r["hour_start"]),
                           kwh_used=r["kwh_used"], billable=bool(r["billable"]),
                           reason=r["reason"]) for r in rows]


@app.get("/batches", response_model=list[BatchOut],
         summary="Audit trail of accepted batches")
def list_batches():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT batch_id, received_at, rows_total, rows_inserted, "
            "rows_deduplicated, archive_sha256, archive_size "
            "FROM batches ORDER BY received_at DESC, batch_id").fetchall()
    finally:
        conn.close()
    return [
        BatchOut(batch_id=r["batch_id"], received_at=r["received_at"],
                 rows_total=r["rows_total"], rows_inserted=r["rows_inserted"],
                 rows_deduplicated=r["rows_deduplicated"],
                 archive_sha256=r["archive_sha256"], archive_size=r["archive_size"],
                 downloadable=r["archive_sha256"] is not None)
        for r in rows
    ]


@app.get("/batches/{batch_id}/download",
         summary="Download the exact original CSV bytes of an accepted batch",
         responses={
             200: {"content": {"text/csv": {}},
                   "description": "Original CSV bytes; SHA-256 verified before sending"},
             404: {"description": "No batch with this batch_id"},
             410: {"description": "Batch predates raw-byte archiving; original is not available"},
             500: {"description": "Archived file missing or corrupt (hash/size mismatch); "
                                  "no partial content is returned"},
         })
def download_batch(batch_id: str):
    if not archive.is_valid_batch_id(batch_id):
        raise HTTPException(status_code=404, detail=f"unknown batch_id: {batch_id}")

    conn = connect()
    try:
        row = conn.execute(
            "SELECT archive_sha256, archive_size FROM batches WHERE batch_id = ?",
            (batch_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown batch_id: {batch_id}")
    if row["archive_sha256"] is None or row["archive_size"] is None:
        # Batch accepted before raw-byte archiving existed.
        raise HTTPException(
            status_code=410,
            detail=("the original CSV of this batch was not archived (batch predates "
                    "raw-byte archiving); download is not available"))

    # Verify the complete file first; only then build the response, so a
    # corrupt or missing archive never yields partial output.
    try:
        data = archive.load_verified(batch_id, row["archive_sha256"], row["archive_size"])
    except archive.ArchiveMissingError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except archive.ArchiveIntegrityError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    headers = {
        "Content-Disposition": f'attachment; filename="{batch_id}.csv"',
        "X-Content-SHA256": row["archive_sha256"],
    }
    return Response(content=data, media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}
