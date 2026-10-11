"""Raw -> Staging -> Curated transformations.

Cleaning rules come from metadata/data_contract.yml (types, nullability, ranges), so the
same logic is never re-implemented in the training code. Every function is rerun-safe:
outputs are rebuilt from the committed raw batches and replaced atomically.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from .config import resolve
from .ingest import _batches_dir, load_checkpoint, new_run_id

log = logging.getLogger("transform")

RAW_REQUIRED_AUDIT = ["_source_seq", "_row_hash"]


class ContractError(ValueError):
    """Raised when the raw data violates a contract rule that is configured to fail."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8")
    os.replace(tmp, path)


def _write_json_atomic(obj: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=False), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------- paths / contract
def staging_path(settings: dict) -> Path:
    return resolve(settings, settings["staging"]["file"])


def rejects_path(settings: dict) -> Path:
    return resolve(settings, settings["staging"]["rejects_file"])


def curated_path(settings: dict) -> Path:
    return resolve(settings, settings["curated"]["file"])


def load_contract(settings: dict) -> dict:
    p = resolve(settings, settings["paths"]["metadata_dir"]) / "data_contract.yml"
    if not p.exists():
        raise FileNotFoundError("metadata/data_contract.yml missing; run `python -m src.cli profile` first")
    return yaml.safe_load(p.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- raw layer reader
def read_raw(settings: dict) -> pd.DataFrame:
    """Concatenate all committed raw batches (as text) in batch order."""
    cp = load_checkpoint(settings)
    if not cp or not cp.get("committed_batches"):
        raise FileNotFoundError("No committed raw batches; run `python -m src.cli ingest` first")
    if len(cp["committed_batches"]) != cp["num_batches"]:
        raise ValueError(
            f"Ingestion incomplete: {len(cp['committed_batches'])}/{cp['num_batches']} batches committed. "
            "Finish `python -m src.cli ingest` before transforming."
        )
    frames = []
    for b in sorted(cp["committed_batches"], key=lambda x: x["batch_id"]):
        path = _batches_dir(settings) / f"batch_{b['batch_id']:04d}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Committed batch file missing: {path}")
        frames.append(pd.read_csv(path, dtype=str, keep_default_na=False))
    first = list(frames[0].columns)
    for b, f in zip(sorted(cp["committed_batches"], key=lambda x: x["batch_id"]), frames):
        if list(f.columns) != first:
            raise ContractError(
                f"Batch {b['batch_id']} has a different schema than batch 1: "
                f"{sorted(set(f.columns) ^ set(first))}"
            )
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------- staging
def clean_frame(raw: pd.DataFrame, contract: dict, run_id: str, processed_at: str):
    """Type, standardize and validate raw rows.

    Returns (clean, rejects, report). Rows that violate a quarantine rule are isolated in
    `rejects` with a reject_reason; they never reach staging.
    """
    required = contract["required_columns"]
    missing = [c["name"] for c in required if c["name"] not in raw.columns]
    missing += [c for c in RAW_REQUIRED_AUDIT if c not in raw.columns]
    if missing:
        raise ContractError(f"Raw data is missing required columns: {missing}")

    on_viol = contract.get("on_violation", {})
    masks: dict[str, pd.Series] = {}
    typed = pd.DataFrame(index=raw.index)
    range_warnings: dict[str, int] = {}

    for spec in required:
        name, ctype = spec["name"], spec["type"]
        nullable = bool(spec.get("nullable", False))
        s = raw[name].astype("string").str.strip()
        s = s.mask(s == "", pd.NA)  # standardize empty strings to null
        was_null = s.isna()

        if ctype in ("integer", "float"):
            num = pd.to_numeric(s, errors="coerce").astype("float64")
            bad = s.notna() & num.isna()                      # unparseable text
            bad |= np.isinf(num.fillna(0.0))                  # inf / -inf
            if ctype == "integer":
                bad |= num.notna() & (num % 1 != 0)           # non-integral value in an integer column
            num = num.where(~bad)
            if bad.any():
                masks[f"type_violation:{name}"] = bad
            lo, hi = spec.get("min"), spec.get("max")
            if lo is not None and hi is not None:
                out = num.notna() & ((num < lo) | (num > hi))
                if out.any():
                    range_warnings[name] = int(out.sum())
                    if on_viol.get("out_of_range") == "quarantine":
                        masks[f"out_of_range:{name}"] = out
            typed[name] = num.round().astype("Int64") if ctype == "integer" else num
        else:
            typed[name] = s

        if not nullable and was_null.any():
            masks[f"null_in_non_nullable:{name}"] = was_null

    dup = raw["_row_hash"].duplicated(keep="first")
    if dup.any():
        masks["duplicate_record_hash"] = dup

    if masks:
        reject_mask = pd.concat(masks, axis=1).any(axis=1)
        reason_frame = pd.concat(masks, axis=1)[reject_mask]
        reasons = reason_frame.apply(lambda r: ";".join(r.index[r.to_numpy()]), axis=1)
    else:
        reject_mask = pd.Series(False, index=raw.index)
        reasons = pd.Series(dtype="object")

    clean = pd.DataFrame(index=raw.index)
    clean["source_row_key"] = pd.to_numeric(raw["_source_seq"]).astype("int64")
    clean["record_hash"] = raw["_row_hash"].astype(object)
    clean["batch_id"] = pd.to_numeric(raw["batch_id"]).astype("int64") if "batch_id" in raw.columns else 0
    for spec in required:
        clean[spec["name"]] = typed[spec["name"]]
    clean["pipeline_run_id"] = run_id
    clean["processed_at_utc"] = processed_at
    good = clean[~reject_mask].sort_values("source_row_key", kind="mergesort").reset_index(drop=True)

    rejects = raw[reject_mask].copy()
    rejects["reject_reason"] = reasons
    rejects["pipeline_run_id"] = run_id
    rejects["processed_at_utc"] = processed_at

    report = {
        "pipeline_run_id": run_id,
        "processed_at_utc": processed_at,
        "rows_in": int(len(raw)),
        "rows_staged": int(len(good)),
        "rows_rejected": int(reject_mask.sum()),
        "reject_reasons": {k: int(v.sum()) for k, v in masks.items()},
        "range_warnings": range_warnings,
    }
    return good, rejects, report


def build_staging(settings: dict, run_id: str | None = None) -> dict:
    run_id = run_id or new_run_id()
    contract = load_contract(settings)
    raw = read_raw(settings)
    good, rejects, report = clean_frame(raw, contract, run_id, _utc_now())
    if report["range_warnings"]:
        log.warning("out-of-range values (warn only): %s", report["range_warnings"])
    if report["rows_rejected"]:
        log.warning("%d rows quarantined: %s", report["rows_rejected"], report["reject_reasons"])
    if len(good) == 0:
        raise ContractError("No valid rows left after validation; refusing to write an empty staging layer")
    _write_csv_atomic(good, staging_path(settings))
    _write_csv_atomic(rejects, rejects_path(settings))
    _write_json_atomic(report, staging_path(settings).with_name("staging_report.json"))
    log.info("staging: %d rows in, %d staged, %d rejected", report["rows_in"], report["rows_staged"], report["rows_rejected"])
    return report


# ---------------------------------------------------------------- typed readers
def apply_types(df: pd.DataFrame, contract: dict, columns: list[str]) -> pd.DataFrame:
    spec = {c["name"]: c["type"] for c in contract["required_columns"]}
    out = df.copy()
    for c in columns:
        if c in ("source_row_key", "batch_id") or spec.get(c) == "integer":
            out[c] = pd.to_numeric(out[c]).astype("int64")
        elif spec.get(c) == "float":
            out[c] = pd.to_numeric(out[c]).astype("float64")
    return out


def read_staging(settings: dict) -> pd.DataFrame:
    p = staging_path(settings)
    if not p.exists():
        raise FileNotFoundError("Staging layer missing; run `python -m src.cli stage` first")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    return apply_types(df, load_contract(settings), list(df.columns))


# ---------------------------------------------------------------- curated
def model_columns(settings: dict) -> tuple[list[str], str]:
    m = settings["model"]
    return list(m["categorical_features"]) + list(m["numeric_features"]), m["target"]


def build_curated(settings: dict) -> dict:
    contract = load_contract(settings)
    features, target = model_columns(settings)
    excluded = set(settings["model"].get("excluded_columns", [])) | set(contract.get("excluded_from_modeling", []))
    clash = [c for c in features + [target] if c in excluded]
    if clash:
        raise ContractError(f"Columns excluded for leakage are used by the model config: {clash}")

    st = read_staging(settings)
    keep = ["source_row_key", "record_hash"] + features + [target]
    absent = [c for c in keep if c not in st.columns]
    if absent:
        raise ContractError(f"Staging layer lacks columns required by the model config: {absent}")
    cur = st[keep].sort_values("source_row_key", kind="mergesort").reset_index(drop=True)  # deterministic order
    if cur[keep].isna().any().any():
        raise ContractError("Curated data contains nulls; staging validation should have removed them")
    if not cur["source_row_key"].is_unique:
        raise ContractError("source_row_key is not unique in curated data")
    _write_csv_atomic(cur, curated_path(settings))
    info = {"rows": int(len(cur)), "columns": list(cur.columns), "excluded_columns": sorted(excluded)}
    log.info("curated: %d rows, columns=%s, excluded=%s", info["rows"], info["columns"], info["excluded_columns"])
    return info


def read_curated(settings: dict) -> pd.DataFrame:
    p = curated_path(settings)
    if not p.exists():
        raise FileNotFoundError("Curated layer missing; run `python -m src.cli curate` first")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    return apply_types(df, load_contract(settings), list(df.columns))
