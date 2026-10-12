"""Tests for staging, curated layer, deterministic split and baseline comparison.

Self-contained: builds a small synthetic raw layer in a temp folder, so no real dataset is needed.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from src import model, transform

CONTRACT = {
    "excluded_from_modeling": ["Source IP"],
    "required_columns": [
        {"name": "Highest Layer", "type": "string", "nullable": False},
        {"name": "Transport Layer", "type": "string", "nullable": False},
        {"name": "Source IP", "type": "integer", "nullable": False, "min": 0, "max": 1},
        {"name": "Dest IP", "type": "string", "nullable": False},
        {"name": "Source Port", "type": "integer", "nullable": False, "min": 0, "max": 65530},
        {"name": "Dest Port", "type": "integer", "nullable": False, "min": 0, "max": 50890},
        {"name": "Packet Length", "type": "integer", "nullable": False, "min": 42, "max": 1514},
        {"name": "Packets/Time", "type": "float", "nullable": False, "min": 0, "max": 484.4},
        {"name": "target", "type": "integer", "nullable": False, "min": 0, "max": 1},
    ],
    "on_violation": {"out_of_range": "warn"},
}


def _raw_rows(n: int = 400, seed: int = 3) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    t = rng.integers(0, 2, n)
    df = pd.DataFrame({
        "Highest Layer": np.where(t == 1, "TCP", rng.choice(["DNS", "NTP"], n)),
        "Transport Layer": rng.choice(["TCP", "UDP"], n),
        "Source IP": t,
        "Dest IP": rng.choice(["192.168.1.1", "192.168.1.2", "192.168.1.3"], n),
        "Source Port": rng.integers(0, 60000, n),
        "Dest Port": rng.choice([0, 53, 80], n),
        "Packet Length": rng.integers(42, 1500, n),
        "Packets/Time": np.round(rng.random(n) * 400, 1),
        "target": t,
    })
    df = df.drop_duplicates().reset_index(drop=True)
    df["_source_seq"] = np.arange(len(df))
    df["_row_hash"] = [f"{i:016x}" for i in range(len(df))]
    df["batch_id"] = 1
    return df.astype(str)


@pytest.fixture()
def settings(tmp_path: Path) -> dict:
    root = tmp_path
    for d in ("data/raw/batches", "data/staging", "data/curated", "data/quarantine",
              "metadata", "state", "outputs/model"):
        (root / d).mkdir(parents=True)
    raw = _raw_rows()
    half = len(raw) // 2
    raw.iloc[:half].to_csv(root / "data/raw/batches/batch_0001.csv", index=False)
    second = raw.iloc[half:].copy()
    second["batch_id"] = "2"
    second.to_csv(root / "data/raw/batches/batch_0002.csv", index=False)
    (root / "state/ingest_checkpoint.json").write_text(json.dumps({
        "num_batches": 2,
        "committed_batches": [{"batch_id": 1}, {"batch_id": 2}],
    }))
    (root / "metadata/data_contract.yml").write_text(yaml.safe_dump(CONTRACT))
    return {
        "_root": str(root),
        "paths": {"raw_dir": "data/raw", "metadata_dir": "metadata",
                  "checkpoint_file": "state/ingest_checkpoint.json"},
        "reproducibility": {"random_seed": 42, "test_size": 0.2, "metric_tolerance": 0.01},
        "staging": {"file": "data/staging/staging.csv", "rejects_file": "data/quarantine/staging_rejects.csv"},
        "curated": {"file": "data/curated/curated.csv"},
        "model": {
            "target": "target",
            "categorical_features": ["Highest Layer", "Transport Layer", "Dest IP"],
            "numeric_features": ["Source Port", "Dest Port", "Packet Length", "Packets/Time"],
            "excluded_columns": ["Source IP"],
            "hyperparameters": {"max_depth": 5},
            "split_manifest": "metadata/split_manifest.json",
            "metrics_file": "outputs/metrics.json",
            "comparison_file": "outputs/metrics_comparison.json",
            "predictions_file": "outputs/predictions.csv",
            "model_file": "outputs/model/model.joblib",
        },
    }


def test_clean_frame_quarantines_invalid_rows():
    raw = _raw_rows(50)
    raw.loc[0, "Source Port"] = "abc"      # type violation
    raw.loc[1, "Dest IP"] = ""             # null in non-nullable column
    raw.loc[2, "Packet Length"] = "99999"  # out of range: warn only, row kept
    good, rejects, report = transform.clean_frame(raw, CONTRACT, "run_t", "2026-01-01T00:00:00+00:00")
    assert report["rows_rejected"] == 2
    assert set(rejects["reject_reason"]) == {"type_violation:Source Port", "null_in_non_nullable:Dest IP"}
    assert report["range_warnings"] == {"Packet Length": 1}
    assert len(good) == len(raw) - 2
    assert {"pipeline_run_id", "processed_at_utc", "source_row_key", "record_hash"} <= set(good.columns)
    assert good["source_row_key"].is_monotonic_increasing


def test_clean_frame_missing_column_is_a_hard_failure():
    raw = _raw_rows(20).drop(columns=["Dest Port"])
    with pytest.raises(transform.ContractError, match="Dest Port"):
        transform.clean_frame(raw, CONTRACT, "run_t", "t")


def test_clean_frame_quarantines_duplicate_hash():
    raw = _raw_rows(20)
    raw.loc[5, "_row_hash"] = raw.loc[4, "_row_hash"]
    _, rejects, report = transform.clean_frame(raw, CONTRACT, "run_t", "t")
    assert report["reject_reasons"] == {"duplicate_record_hash": 1}
    assert len(rejects) == 1


def test_curated_excludes_leaky_column_and_is_ordered(settings):
    transform.build_staging(settings)
    transform.build_curated(settings)
    cur = transform.read_curated(settings)
    assert "Source IP" not in cur.columns
    assert cur["source_row_key"].is_monotonic_increasing and cur["source_row_key"].is_unique


def test_curated_refuses_leaky_feature(settings):
    settings["model"]["numeric_features"].append("Source IP")
    transform.build_staging(settings)
    with pytest.raises(transform.ContractError, match="Source IP"):
        transform.build_curated(settings)


def test_pipeline_is_deterministic(settings):
    def run():
        transform.build_staging(settings)
        transform.build_curated(settings)
        res = model.run_training(settings)
        manifest = (Path(settings["_root"]) / "metadata/split_manifest.json").read_text()
        preds = (Path(settings["_root"]) / "outputs/predictions.csv").read_text()
        return res["metrics"], manifest, preds

    m1, man1, p1 = run()
    m2, man2, p2 = run()
    assert m1 == m2 and man1 == man2 and p1 == p2
    manifest = json.loads(man1)
    assert manifest["seed"] == 42 and manifest["train"]["rows"] + manifest["test"]["rows"] == manifest["rows_total"]


def test_comparison_passes_then_fails_outside_tolerance(settings):
    transform.build_staging(settings)
    transform.build_curated(settings)
    metrics = model.run_training(settings)["metrics"]
    exp_path = Path(settings["_root"]) / "metadata/expected_metrics.json"
    exp_path.write_text(json.dumps({"metrics": metrics, "tolerance": {"absolute": 0.01}}))
    assert model.compare_metrics(settings)["passed"] is True

    bad = dict(metrics, accuracy=round(metrics["accuracy"] - 0.05, 4))
    exp_path.write_text(json.dumps({"metrics": bad, "tolerance": {"absolute": 0.01}}))
    with pytest.raises(model.ComparisonFailed):
        model.compare_metrics(settings)
