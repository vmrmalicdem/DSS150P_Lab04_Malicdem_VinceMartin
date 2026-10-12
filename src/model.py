"""Deterministic split, training, metrics and baseline comparison."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import random
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier

from .config import resolve
from .ingest import new_run_id
from .source import sha256_file
from .transform import curated_path, model_columns, read_curated

log = logging.getLogger("model")

METRIC_DEFINITIONS = {
    "accuracy": "Share of test rows predicted correctly.",
    "precision": "TP / (TP + FP) for the positive class (target = 1, attack traffic).",
    "recall": "TP / (TP + FN) for the positive class (target = 1).",
    "f1": "Harmonic mean of precision and recall for the positive class.",
    "roc_auc": "Area under the ROC curve from predicted probability of target = 1.",
}


class ComparisonFailed(RuntimeError):
    """Actual metrics are outside the allowed tolerance of the frozen baseline."""


def set_seeds(seed: int) -> None:
    """Set every seed source used by this project (Python, NumPy; sklearn gets random_state)."""
    os.environ["PYTHONHASHSEED"] = str(seed)
    random.seed(seed)
    np.random.seed(seed)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_json(obj: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _ids_sha256(ids) -> str:
    return hashlib.sha256(",".join(str(i) for i in sorted(int(i) for i in ids)).encode("ascii")).hexdigest()


def build_pipeline(settings: dict, seed: int) -> Pipeline:
    m = settings["model"]
    params = dict(m.get("hyperparameters", {}))
    params["random_state"] = seed
    pre = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), list(m["categorical_features"]))],
        remainder="passthrough",  # numeric features pass through unchanged, in configured order
    )
    return Pipeline([("prep", pre), ("clf", DecisionTreeClassifier(**params))])


def compute_metrics(y_true, y_pred, proba) -> dict:
    vals = {
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
        "roc_auc": roc_auc_score(y_true, proba),
    }
    return {k: round(float(v), 4) for k, v in vals.items()}


def run_training(settings: dict, run_id: str | None = None) -> dict:
    run_id = run_id or new_run_id()
    seed = int(settings["reproducibility"]["random_seed"])
    test_size = float(settings["reproducibility"]["test_size"])
    set_seeds(seed)

    features, target = model_columns(settings)
    df = read_curated(settings)
    X, y, ids = df[features], df[target], df["source_row_key"]

    # Same call shape as the Phase I baseline: stratified split, fixed seed.
    Xtr, Xte, ytr, yte, idtr, idte = train_test_split(
        X, y, ids, test_size=test_size, stratify=y, random_state=seed
    )

    manifest = {
        "method": "sklearn.model_selection.train_test_split",
        "stratified_on": target,
        "seed": seed,
        "test_size": test_size,
        "id_column": "source_row_key",
        "curated_file": str(curated_path(settings).relative_to(settings["_root"])).replace("\\", "/"),
        "curated_sha256": sha256_file(curated_path(settings)),
        "rows_total": int(len(df)),
        "train": {
            "rows": int(len(Xtr)),
            "class_counts": {str(k): int(v) for k, v in ytr.value_counts().sort_index().items()},
            "ids_sha256": _ids_sha256(idtr),
        },
        "test": {
            "rows": int(len(Xte)),
            "class_counts": {str(k): int(v) for k, v in yte.value_counts().sort_index().items()},
            "ids_sha256": _ids_sha256(idte),
        },
        "libraries": {"scikit-learn": sklearn.__version__, "pandas": pd.__version__, "numpy": np.__version__},
    }
    manifest_path = resolve(settings, settings["model"]["split_manifest"])
    _write_json(manifest, manifest_path)  # no timestamps: identical input gives an identical file

    pipe = build_pipeline(settings, seed)
    pipe.fit(Xtr, ytr)
    pred = pipe.predict(Xte)
    proba = pipe.predict_proba(Xte)[:, 1]
    metrics = compute_metrics(yte, pred, proba)

    m = settings["model"]
    out = {
        "pipeline_run_id": run_id,
        "created_at_utc": _utc_now(),
        "task": "binary classification",
        "target": target,
        "model": {
            "family": "DecisionTreeClassifier",
            "hyperparameters": {**m.get("hyperparameters", {}), "random_state": seed},
        },
        "features": {
            "categorical": list(m["categorical_features"]),
            "numeric": list(m["numeric_features"]),
            "excluded": list(m.get("excluded_columns", [])),
        },
        "split_manifest": str(manifest_path.relative_to(settings["_root"])).replace("\\", "/"),
        "metrics": metrics,
        "metric_definitions": METRIC_DEFINITIONS,
        "environment": {"python": platform.python_version(), **manifest["libraries"]},
    }
    _write_json(out, resolve(settings, m["metrics_file"]))

    pred_path = resolve(settings, m["predictions_file"])
    pred_path.parent.mkdir(parents=True, exist_ok=True)
    preds = pd.DataFrame(
        {"source_row_key": idte.to_numpy(), "y_true": yte.to_numpy(), "y_pred": pred, "proba_attack": proba.round(6)}
    ).sort_values("source_row_key", kind="mergesort")
    preds.to_csv(pred_path, index=False)

    model_path = resolve(settings, m["model_file"])
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_path)
    log.info("trained %s on %d rows, tested on %d rows: %s", out["model"]["family"], len(Xtr), len(Xte), metrics)
    return out


def compare_metrics(settings: dict) -> dict:
    """Compare outputs/metrics.json with metadata/expected_metrics.json (automatic baseline check)."""
    actual_path = resolve(settings, settings["model"]["metrics_file"])
    expected_path = resolve(settings, settings["paths"]["metadata_dir"]) / "expected_metrics.json"
    for p in (actual_path, expected_path):
        if not p.exists():
            raise FileNotFoundError(f"{p.name} not found at {p}")
    actual = json.loads(actual_path.read_text(encoding="utf-8"))["metrics"]
    expected_doc = json.loads(expected_path.read_text(encoding="utf-8"))
    expected = expected_doc["metrics"]
    tol = float(expected_doc.get("tolerance", {}).get("absolute", settings["reproducibility"]["metric_tolerance"]))

    rows, ok = {}, True
    for name, exp in expected.items():
        if exp is None:
            raise ValueError(f"expected_metrics.json has no value for '{name}'; freeze the baseline first")
        if name not in actual:
            raise ValueError(f"metrics.json lacks metric '{name}'")
        diff = abs(actual[name] - exp)
        within = diff <= tol + 1e-12
        same_3dp = round(actual[name], 3) == round(exp, 3)
        ok &= within
        rows[name] = {"expected": exp, "actual": actual[name], "abs_diff": round(diff, 6),
                      "within_tolerance": within, "equal_to_3_decimals": same_3dp}
    report = {"tolerance_absolute": tol, "passed": ok, "metrics": rows, "compared_at_utc": _utc_now()}
    _write_json(report, resolve(settings, settings["model"]["comparison_file"]))
    if not ok:
        bad = [k for k, v in rows.items() if not v["within_tolerance"]]
        raise ComparisonFailed(f"Metrics outside tolerance ±{tol}: {bad}")
    return report
