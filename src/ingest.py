"""Full and incremental (simulated) ingestion with checkpointing and duplicate prevention.

Incremental design (lab simulation, not a natural business watermark):
  * The source has a stable order, so each row gets `_source_seq` = its 0-based position.
  * The source is cut into `num_batches` contiguous, equally sized batches (deterministic).
  * The checkpoint (state/ingest_checkpoint.json) lists committed batches and the last committed sequence.
  * A batch file is written to a temp name, renamed, and only then recorded in the checkpoint,
    so a crash never leaves a batch marked committed without its data (and a rerun redoes it).
  * Logical duplicates are detected by the business key (if configured) or a deterministic row hash and are
    quarantined rather than appended.
"""
from __future__ import annotations

import json
import logging
import math
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import resolve
from .source import load_manifest, read_source, sha256_file, source_path

log = logging.getLogger("ingest")

SEQ_COL = "_source_seq"
AUDIT_COLS = ["pipeline_run_id", "ingested_at_utc", "source_file", "source_checksum", "batch_id"]


class SourceChangedError(RuntimeError):
    pass


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:6]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _check_source(settings: dict) -> tuple[Path, str]:
    p = source_path(settings)
    checksum = sha256_file(p)
    expected = load_manifest(settings)["checksum"]["value"]
    if checksum != expected:
        raise SourceChangedError(
            f"Source checksum {checksum} does not match manifest {expected}. Refusing to ingest."
        )
    return p, checksum


def row_hash(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    h = pd.util.hash_pandas_object(df[columns].astype(str), index=False).astype("uint64")
    return h.map("{:016x}".format)


def _write_run_log(settings: dict, run_id: str, record: dict) -> None:
    d = resolve(settings, settings["paths"]["run_log_dir"])
    _atomic_write_text(d / f"{run_id}.json", json.dumps(record, indent=2))


# ---------------------------------------------------------------------------------------------- full
def full_ingest(settings: dict, run_id: str | None = None) -> dict:
    run_id = run_id or new_run_id()
    src, checksum = _check_source(settings)
    out_dir = resolve(settings, settings["paths"]["raw_dir"]) / run_id / "full"
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / src.name
    shutil.copyfile(src, dest)  # byte-for-byte copy; source is never modified
    if sha256_file(dest) != checksum:
        raise RuntimeError("Raw copy checksum mismatch after copy")
    with open(dest, "rb") as fh:
        rows = sum(1 for _ in fh) - 1
    audit = {
        "pipeline_run_id": run_id,
        "mode": "full",
        "ingested_at_utc": _utc_now(),
        "source_file": settings["dataset"]["source_file"],
        "source_checksum": checksum,
        "raw_file": str(dest.relative_to(settings["_root"])),
        "line_count_excluding_header": rows,
    }
    _atomic_write_text(out_dir / "_audit.json", json.dumps(audit, indent=2))
    _write_run_log(settings, run_id, audit)
    log.info("full ingest %s: %s rows copied to %s", run_id, rows, audit["raw_file"])
    return audit


# ----------------------------------------------------------------------------------------- incremental
def _checkpoint_path(settings: dict) -> Path:
    return resolve(settings, settings["paths"]["checkpoint_file"])


def load_checkpoint(settings: dict) -> dict | None:
    p = _checkpoint_path(settings)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _batches_dir(settings: dict) -> Path:
    return resolve(settings, settings["paths"]["raw_dir"]) / "batches"


class IntentionalStop(RuntimeError):
    """Raised by --stop-after to simulate a run that is interrupted after N committed batches."""


class BatchValidationError(RuntimeError):
    pass


def _committed_files(settings: dict, cp: dict) -> list[Path]:
    return [resolve(settings, b["file"]) for b in cp["committed_batches"]]


def _clean_orphans(settings: dict, cp: dict) -> list[str]:
    """Delete batch files that are not listed in the checkpoint (written, then the run died before commit)."""
    keep = {p.resolve() for p in _committed_files(settings, cp)}
    removed = []
    for tmp in _batches_dir(settings).glob("*.tmp"):
        tmp.unlink()
        removed.append(tmp.name)
    for f in _batches_dir(settings).glob("batch_*.csv"):
        if f.resolve() not in keep:
            f.unlink()
            removed.append(f.name)
    if removed:
        log.warning("removed uncommitted leftovers from an interrupted run: %s", removed)
    return removed


def _committed_hashes(settings: dict, cp: dict, hash_col: str) -> set[str]:
    seen: set[str] = set()
    for f in _committed_files(settings, cp):
        seen.update(pd.read_csv(f, usecols=[hash_col], dtype=str)[hash_col])
    return seen


def _validate_batch(path: Path, expected: pd.DataFrame, hash_col: str, seq_start: int, seq_end: int) -> None:
    """Re-read the written batch and prove it matches what we meant to write, before the checkpoint moves."""
    back = pd.read_csv(path, dtype=str, keep_default_na=False)
    problems = []
    if len(back) != len(expected):
        problems.append(f"row count {len(back)} != expected {len(expected)}")
    else:
        if list(back[hash_col]) != list(expected[hash_col]):
            problems.append("row hashes differ from the in-memory batch")
        if list(back[SEQ_COL].astype(int)) != list(expected[SEQ_COL]):
            problems.append("source sequence differs from the in-memory batch")
    for col in AUDIT_COLS:
        if col not in back.columns or (back[col] == "").any():
            problems.append(f"audit column {col} missing or empty")
    if len(back) and (int(back[SEQ_COL].astype(int).min()) < seq_start or int(back[SEQ_COL].astype(int).max()) > seq_end):
        problems.append("source sequence outside the batch range")
    if problems:
        raise BatchValidationError(f"{path.name}: " + "; ".join(problems))


def incremental_ingest(settings: dict, run_id: str | None = None, force: bool = False,
                       stop_after: int | None = None) -> dict:
    run_id = run_id or new_run_id()
    src, checksum = _check_source(settings)
    ing = settings["ingestion"]
    hash_col = ing["hash_column"]
    num_batches = int(ing["num_batches"])
    if num_batches < 3:
        raise ValueError("num_batches must be >= 3")

    cp = load_checkpoint(settings)
    if force and cp:
        log.warning("--force: clearing checkpoint and raw batches")
        shutil.rmtree(_batches_dir(settings), ignore_errors=True)
        qdir = resolve(settings, settings["paths"]["quarantine_dir"])
        for f in qdir.glob("dupes_batch_*.csv"):
            f.unlink()
        _checkpoint_path(settings).unlink()
        cp = None
    if cp and cp["source_checksum"] != checksum:
        raise SourceChangedError("Checkpoint belongs to a different source checksum; rerun with --force")

    df_iter_probe = pd.read_csv(src, usecols=[0], chunksize=ing["chunk_rows"],
                                encoding=settings["dataset"]["encoding"], sep=settings["dataset"]["delimiter"])
    total_rows = sum(len(c) for c in df_iter_probe)
    batch_size = math.ceil(total_rows / num_batches)

    if cp is None:
        cp = {
            "source_checksum": checksum,
            "total_rows": total_rows,
            "num_batches": num_batches,
            "batch_size": batch_size,
            "strategy": "row_sequence_simulation",
            "last_committed_seq": -1,
            "committed_batches": [],
        }
    elif cp["total_rows"] != total_rows or cp["num_batches"] != num_batches:
        raise SourceChangedError("Batching parameters changed since last run; rerun with --force")

    done = {b["batch_id"] for b in cp["committed_batches"]}
    _batches_dir(settings).mkdir(parents=True, exist_ok=True)
    orphans = _clean_orphans(settings, cp)
    key = settings["dataset"].get("business_key") or []
    committed = _committed_hashes(settings, cp, hash_col) if not key else None
    committed_keys = None
    if key:
        committed_keys = set()
        for f in _committed_files(settings, cp):
            committed_keys.update(map(tuple, pd.read_csv(f, usecols=key, dtype=str).itertuples(index=False)))

    skipped, written = [], []
    reader = pd.read_csv(src, chunksize=batch_size, low_memory=False,
                         encoding=settings["dataset"]["encoding"], sep=settings["dataset"]["delimiter"])
    for i, chunk in enumerate(reader, start=1):
        if i in done:
            skipped.append(i)
            continue
        start = (i - 1) * batch_size
        chunk = chunk.reset_index(drop=True)
        src_cols = list(chunk.columns)
        chunk[SEQ_COL] = range(start, start + len(chunk))
        chunk[hash_col] = row_hash(chunk, src_cols)
        if key:
            keys_str = list(map(tuple, chunk[key].astype(str).itertuples(index=False)))
            dup = chunk.duplicated(subset=key, keep="first") | pd.Series([k in committed_keys for k in keys_str],
                                                                         index=chunk.index)
        else:
            dup = chunk.duplicated(subset=[hash_col], keep="first") | chunk[hash_col].isin(committed)
        dupes, keep = chunk[dup], chunk[~dup].copy()
        keep["pipeline_run_id"] = run_id
        keep["ingested_at_utc"] = _utc_now()
        keep["source_file"] = settings["dataset"]["source_file"]
        keep["source_checksum"] = checksum
        keep["batch_id"] = i

        bdir = _batches_dir(settings)
        bdir.mkdir(parents=True, exist_ok=True)
        final = bdir / f"batch_{i:04d}.csv"
        tmp = final.with_suffix(".csv.tmp")
        keep.to_csv(tmp, index=False)
        os.replace(tmp, final)
        try:
            _validate_batch(final, keep, hash_col, start, start + len(chunk) - 1)
        except BatchValidationError:
            final.unlink(missing_ok=True)  # never leave an unvalidated batch behind; checkpoint stays put
            raise
        if len(dupes):
            qdir = resolve(settings, settings["paths"]["quarantine_dir"])
            qdir.mkdir(parents=True, exist_ok=True)
            dupes.to_csv(qdir / f"dupes_batch_{i:04d}.csv", index=False)
        if committed is not None:
            committed.update(keep[hash_col])
        if committed_keys is not None:
            committed_keys.update(map(tuple, keep[key].astype(str).itertuples(index=False)))

        cp["committed_batches"].append({
            "batch_id": i,
            "seq_start": start,
            "seq_end": start + len(chunk) - 1,
            "rows_read": len(chunk),
            "rows_written": len(keep),
            "duplicates_quarantined": len(dupes),
            "file": str(final.relative_to(settings["_root"])),
            "pipeline_run_id": run_id,
            "committed_at_utc": _utc_now(),
        })
        cp["last_committed_seq"] = start + len(chunk) - 1
        _atomic_write_text(_checkpoint_path(settings), json.dumps(cp, indent=2))  # commit point
        written.append(i)
        log.info("batch %d committed: %d written, %d quarantined", i, len(keep), len(dupes))
        if stop_after is not None and len(written) >= stop_after:
            _write_run_log(settings, run_id, {
                "pipeline_run_id": run_id, "mode": "incremental", "status": "interrupted_intentionally",
                "stopped_at_utc": _utc_now(), "batches_written": written, "last_committed_seq": cp["last_committed_seq"],
            })
            raise IntentionalStop(
                f"Stopped on purpose after {len(written)} committed batch(es) (--stop-after). "
                "Rerun without the flag to resume."
            )

    summary = {
        "pipeline_run_id": run_id,
        "mode": "incremental",
        "status": "completed",
        "finished_at_utc": _utc_now(),
        "source_checksum": checksum,
        "batches_total": num_batches,
        "batches_written": written,
        "batches_skipped": skipped,
        "uncommitted_leftovers_removed": orphans,
        "last_committed_seq": cp["last_committed_seq"],
        "rows_written_this_run": sum(b["rows_written"] for b in cp["committed_batches"] if b["batch_id"] in written),
    }
    _write_run_log(settings, run_id, summary)
    return summary


def logical_row_count(settings: dict) -> tuple[int, int]:
    """Return (rows, distinct_row_hashes) across all committed batches — equal means no logical duplicates."""
    hash_col = settings["ingestion"]["hash_column"]
    hashes = []
    for f in sorted(_batches_dir(settings).glob("batch_*.csv")):
        hashes.append(pd.read_csv(f, usecols=[hash_col], dtype=str)[hash_col])
    s = pd.concat(hashes) if hashes else pd.Series(dtype=str)
    return len(s), s.nunique()
