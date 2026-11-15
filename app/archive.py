"""Raw-byte archive of accepted CSV batches.

Every accepted batch keeps the exact bytes the gateway uploaded under
``$ARCHIVE_DIR/<batch_id>.csv`` together with its SHA-256 (stored in the
``batches`` table).  The download endpoint re-reads the whole file and
verifies size and SHA-256 before a single byte is sent to the client.

Crash-safety strategy (two API instances share one archive directory):

* The bytes are first written to a unique ``.tmp-<batch_id>-<uuid>`` file,
  fsynced and then published with ``os.replace`` (atomic on POSIX), so a
  directory never exposes a half-written canonical file.
* The canonical file is made durable *before* the SQLite ingest transaction
  begins.  If the transaction fails/rolls back, the canonical file is
  unlinked; if the unlink or the process itself fails, ``sweep_stale`` at
  startup removes canonical files that no batch row references.
* Sweep only touches files older than ``SWEEP_MIN_AGE_SECONDS`` so files
  belonging to an in-flight ingest of the other instance are never removed.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
import uuid

BATCH_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SWEEP_MIN_AGE_SECONDS = 3600
_TMP_PREFIX = ".tmp-"


class ArchiveError(Exception):
    """Base class for archive storage problems."""


class ArchiveMissingError(ArchiveError):
    """The archived file of a registered batch is not on disk."""


class ArchiveIntegrityError(ArchiveError):
    """The archived file fails the size / SHA-256 verification."""


def is_valid_batch_id(batch_id: str) -> bool:
    return bool(BATCH_ID_RE.match(batch_id))


def archive_dir() -> str:
    # Read at call time so tests / containers can relocate the archive.
    return os.environ.get("ARCHIVE_DIR", "/data/archives")


def canonical_path(batch_id: str) -> str:
    if not is_valid_batch_id(batch_id):
        # Never build a path from an unvalidated id (path traversal guard).
        raise ArchiveError(f"malformed batch_id: {batch_id!r}")
    return os.path.join(archive_dir(), f"{batch_id}.csv")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: str) -> None:
    """Make a rename/unlink inside the directory durable."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def store_original(batch_id: str, data: bytes) -> tuple[str, int]:
    """Durably store the raw uploaded bytes for a batch.

    Returns ``(sha256_hex, size_bytes)``.  The file appears at its canonical
    path atomically; callers must only register the batch in the database
    once this returns successfully.
    """
    directory = archive_dir()
    os.makedirs(directory, exist_ok=True)
    final_path = canonical_path(batch_id)
    tmp_path = os.path.join(directory, f"{_TMP_PREFIX}{batch_id}-{uuid.uuid4().hex}")

    digest = sha256_hex(data)
    size = len(data)
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)
        _fsync_dir(directory)
    except BaseException:
        # A failed write must leave neither a temp file nor a canonical one.
        for path in (tmp_path, final_path):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        raise
    return digest, size


def discard_original(batch_id: str) -> None:
    """Best-effort removal of the canonical file after a DB rollback."""
    try:
        os.unlink(canonical_path(batch_id))
        _fsync_dir(archive_dir())
    except FileNotFoundError:
        pass


def load_verified(batch_id: str, expected_sha256: str, expected_size: int) -> bytes:
    """Read a whole archived file and verify it before returning any byte.

    Raises ArchiveMissingError when the file is gone and
    ArchiveIntegrityError when size or SHA-256 does not match the database
    record.  The caller turns these into error responses, so partial
    content is never streamed to a downloader.
    """
    path = canonical_path(batch_id)
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except FileNotFoundError as exc:
        raise ArchiveMissingError(f"archived original for batch {batch_id} is missing") from exc

    if len(data) != expected_size:
        raise ArchiveIntegrityError(
            f"archived original for batch {batch_id} is corrupt: "
            f"size {len(data)} != recorded {expected_size}")
    actual = sha256_hex(data)
    if actual != expected_sha256:
        raise ArchiveIntegrityError(
            f"archived original for batch {batch_id} failed SHA-256 verification "
            f"(file {actual} != recorded {expected_sha256})")
    return data


def sweep_stale(known_batch_ids: set[str]) -> list[str]:
    """Remove leftovers of interrupted ingests at startup.

    * ``.tmp-*`` files are interrupted writes that never reached their
      canonical name.
    * Canonical ``<batch_id>.csv`` files without a matching batch row are
      archives whose database commit did not happen (process exited in the
      middle, or rollback cleanup failed).

    Only files older than SWEEP_MIN_AGE_SECONDS are considered, which keeps
    files of an ingest currently running on the other instance untouched.
    Returns the list of removed paths (mainly useful for tests/logs).
    """
    directory = archive_dir()
    removed: list[str] = []
    try:
        entries = os.listdir(directory)
    except FileNotFoundError:
        return removed

    now = time.time()
    for name in entries:
        path = os.path.join(directory, name)
        try:
            age = now - os.path.getmtime(path)
            if age < SWEEP_MIN_AGE_SECONDS:
                continue
            if name.startswith(_TMP_PREFIX):
                stale = True
            elif name.endswith(".csv"):
                stale = name[:-4] not in known_batch_ids
            else:
                stale = False
            if stale:
                os.unlink(path)
                removed.append(path)
        except FileNotFoundError:
            continue  # the other instance swept it first
        except OSError:
            continue
    if removed:
        _fsync_dir(directory)
    return removed
