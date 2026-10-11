"""Source-system control: checksum/manifest, profiling, and data contract generation."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from .config import resolve

LEAK_HINTS = re.compile(r"(^|[\s_])(id|ip|flow|timestamp|time|date|port|label|class|attack)([\s_]|$)", re.I)
TIME_HINTS = re.compile(r"(^|[\s_])(timestamp|datetime|date|time)([\s_]|$)", re.I)


def sha256_file(path: Path, block: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(block), b""):
            h.update(chunk)
    return h.hexdigest()


def source_path(settings: dict) -> Path:
    p = resolve(settings, settings["dataset"]["source_file"])
    if not p.exists():
        raise FileNotFoundError(
            f"Source dataset not found at {p.relative_to(settings['_root'])}. "
            "Follow data/source/README.md to place it."
        )
    return p


def read_source(settings: dict, **kwargs) -> pd.DataFrame:
    ds = settings["dataset"]
    df = pd.read_csv(
        source_path(settings), encoding=ds["encoding"], sep=ds["delimiter"], low_memory=False, **kwargs
    )
    df.columns = df.columns.str.strip()
    return df


def detect_target(settings: dict, columns: list[str]) -> str | None:
    ds = settings["dataset"]
    if ds.get("target_column"):
        if ds["target_column"] not in columns:
            raise ValueError(f"Configured target_column {ds['target_column']!r} not in source columns")
        return ds["target_column"]
    lowered = {c.strip().lower(): c for c in columns}
    for cand in ds.get("target_candidates", []):
        if cand in lowered:
            return lowered[cand]
    return None


def write_manifest(settings: dict) -> dict:
    p = source_path(settings)
    with open(p, "rb") as fh:
        rows = sum(1 for _ in fh) - 1
    manifest = {
        "dataset_id": settings["dataset"]["id"],
        "title": settings["dataset"]["title"],
        "source_url": settings["dataset"]["url"],
        "owner": settings["dataset"]["owner"],
        "expected_filename": p.name,
        "placement": settings["dataset"]["source_file"],
        "format": "csv",
        "size_bytes": p.stat().st_size,
        "line_count_excluding_header": rows,
        "checksum": {"algorithm": "sha256", "value": sha256_file(p)},
        "redistributable": False,
        "redistribution_note": "Third-party Kaggle dataset; check the license on the dataset page. "
        "Raw file is not committed to Git.",
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    out = resolve(settings, settings["paths"]["metadata_dir"]) / "source_manifest.yml"
    out.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return manifest


def load_manifest(settings: dict) -> dict:
    p = resolve(settings, settings["paths"]["metadata_dir"]) / "source_manifest.yml"
    if not p.exists():
        raise FileNotFoundError("metadata/source_manifest.yml missing; run `python -m src.cli manifest` first")
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def verify_source(settings: dict) -> tuple[bool, str, str]:
    expected = load_manifest(settings)["checksum"]["value"]
    actual = sha256_file(source_path(settings))
    return expected == actual, expected, actual


def profile_source(settings: dict) -> dict:
    df = read_source(settings)
    n = len(df)
    target = detect_target(settings, list(df.columns))
    cols = []
    for c in df.columns:
        s = df[c]
        info = {
            "name": c,
            "dtype": str(s.dtype),
            "nulls": int(s.isna().sum()),
            "null_rate": round(float(s.isna().mean()), 6),
            "distinct": int(s.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(s):
            fin = s[np.isfinite(s.astype("float64"))]
            info["min"], info["max"] = _num(fin.min()), _num(fin.max())
            info["inf_count"] = int(np.isinf(s.astype("float64")).sum())
        else:
            info["sample"] = [str(v) for v in s.dropna().unique()[:5]]
        info["candidate_key"] = bool(n > 0 and info["distinct"] == n and info["nulls"] == 0)
        info["flags"] = _flags(c, info)
        if target and c != target and 1 < info["distinct"] <= 1000:
            if df.groupby(c)[target].nunique().max() == 1:
                info["flags"].append("leaks target")
        cols.append(info)
    key = settings["dataset"].get("business_key") or []
    prof = {
        "rows": n,
        "columns": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "duplicate_business_key_rows": int(df.duplicated(subset=key).sum()) if key else None,
        "target": target,
        "target_distribution": (
            {str(k): int(v) for k, v in df[target].value_counts(dropna=False).items()} if target else None
        ),
        "columns_detail": cols,
    }
    return prof


def _num(v):
    return None if pd.isna(v) else (int(v) if float(v).is_integer() else float(v))


def _flags(name: str, info: dict) -> list[str]:
    flags = []
    if info["candidate_key"]:
        flags.append("candidate key")
    if info["distinct"] <= 1:
        flags.append("constant")
    if TIME_HINTS.search(name):
        flags.append("timestamp-like")
    if LEAK_HINTS.search(name):
        flags.append("identifier/leakage review")
    if info.get("inf_count"):
        flags.append("contains inf")
    if info["null_rate"] > 0:
        flags.append("has nulls")
    return flags


def write_profile_md(settings: dict, prof: dict) -> Path:
    m = load_manifest(settings)
    L = [
        "# Source Profile",
        "",
        f"- **Dataset:** {m['title']} (`{m['dataset_id']}`)",
        f"- **Source:** {m['source_url']} — owner {m['owner']}",
        f"- **File:** `{m['placement']}` · csv · encoding {settings['dataset']['encoding']} · "
        f"delimiter `{settings['dataset']['delimiter']}` · {m['size_bytes']:,} bytes",
        f"- **SHA-256:** `{m['checksum']['value']}`",
        f"- **Rows × columns:** {prof['rows']:,} × {prof['columns']}",
        f"- **Duplicate rows (all columns):** {prof['duplicate_rows']:,}",
        f"- **Duplicate business-key rows:** "
        f"{prof['duplicate_business_key_rows'] if prof['duplicate_business_key_rows'] is not None else 'n/a (no business key configured)'}",
        "- **Update frequency / availability:** static Kaggle snapshot; no known update schedule.",
        "- **License / redistribution:** check the dataset page; raw file is not published to GitHub.",
        "",
        "## Target variable",
        f"- Detected target: `{prof['target']}`" if prof["target"] else
        "- No target detected automatically. Set `dataset.target_column` in `config/settings.yml`.",
    ]
    if prof["target_distribution"]:
        tot = sum(prof["target_distribution"].values())
        L += ["", "| Class | Count | Share |", "|---|---:|---:|"]
        for k, v in prof["target_distribution"].items():
            L.append(f"| {k} | {v:,} | {v / tot:.2%} |")
        top = max(prof["target_distribution"].values()) / tot
        if top > 0.7:
            L.append(f"\nClass imbalance: majority class is {top:.1%} of rows.")
    L += ["", "## Columns", "", "| Column | dtype | nulls | null % | distinct | min | max | flags |", "|---|---|---:|---:|---:|---|---|---|"]
    for c in prof["columns_detail"]:
        L.append(
            f"| {c['name']} | {c['dtype']} | {c['nulls']:,} | {c['null_rate']:.2%} | {c['distinct']:,} | "
            f"{c.get('min', '')} | {c.get('max', '')} | {', '.join(c['flags'])} |"
        )
    flagged = [c["name"] for c in prof["columns_detail"]
               if "identifier/leakage review" in c["flags"] or "leaks target" in c["flags"]]
    leaks = [c["name"] for c in prof["columns_detail"] if "leaks target" in c["flags"]]
    L += [
        "",
        "## Risks and review items",
        f"- Identifier / leakage candidates to review before modelling: {', '.join(flagged) or 'none flagged'}.",
        f"- Timestamp-like columns: {', '.join(c['name'] for c in prof['columns_detail'] if 'timestamp-like' in c['flags']) or 'none'} "
        "(not assumed reliable; ingestion uses a row-sequence simulation unless `timestamp_column` is set).",
        f"- Columns containing inf: {', '.join(c['name'] for c in prof['columns_detail'] if 'contains inf' in c['flags']) or 'none'}.",
        "- Columns with nulls: " + (", ".join(c["name"] for c in prof["columns_detail"] if c["nulls"]) or "none") + ".",
        f"- Columns that perfectly determine the target (label leakage, excluded from modelling): {', '.join(leaks) or 'none'}.",
    ]
    out = resolve(settings, settings["paths"]["metadata_dir"]) / "source_profile.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    return out


def _contract_type(dtype: str) -> str:
    if dtype.startswith("int"):
        return "integer"
    if dtype.startswith("float"):
        return "float"
    if dtype == "bool":
        return "boolean"
    return "string"


def write_contract(settings: dict, prof: dict) -> Path:
    m = load_manifest(settings)
    c = settings["contract"]
    key = settings["dataset"].get("business_key") or []
    contract = {
        "dataset_id": m["dataset_id"],
        "version": m["checksum"]["value"][:12],
        "checksum": m["checksum"],
        "target": prof["target"],
        "keys": {
            "strategy": "business_key" if key else "row_hash",
            "business_key": key,
            "note": "No natural key assumed; uniqueness is enforced on a deterministic hash of all source columns."
            if not key else "Uniqueness enforced on the configured business key.",
            "excluded_from_modeling": [c["name"] for c in prof["columns_detail"] if "leaks target" in c["flags"]],
        },
        "row_count": {"expected": prof["rows"]},
        "required_columns": [
            {
                "name": col["name"],
                "type": _contract_type(col["dtype"]),
                "nullable": col["nulls"] > 0,
                **({"min": col["min"], "max": col["max"]} if "min" in col else {}),
            }
            for col in prof["columns_detail"]
        ],
        "on_violation": {
            "missing_required_column": c["on_missing_required_column"],
            "type_mismatch": c["on_type_violation"],
            "null_in_non_nullable": c["on_null_in_non_nullable"],
            "duplicate_key": c["on_duplicate_key"],
            "out_of_range": c["on_out_of_range"],
        },
        "notes": "Generated from the profile of the frozen source snapshot; review ranges before enforcing.",
    }
    out = resolve(settings, settings["paths"]["metadata_dir"]) / "data_contract.yml"
    out.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return out
