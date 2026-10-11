"""Test fixtures: a tiny synthetic stand-in for the source in a temp dir (never the real dataset)."""
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


def make_synthetic(path: Path, rows: int = 103, dup_rows: int = 3, seed: int = 0) -> None:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "Flow Duration": rng.integers(1, 10_000, rows),
        "Total Fwd Packets": rng.integers(1, 200, rows),
        "Packet Length Mean": rng.normal(500, 100, rows).round(3),
        "Protocol": rng.choice([6, 17], rows),
        "Label": rng.choice(["BENIGN", "DDoS"], rows, p=[0.3, 0.7]),
    })
    df = pd.concat([df, df.iloc[:dup_rows]], ignore_index=True)  # exact duplicate rows
    df.to_csv(path, index=False)


@pytest.fixture()
def settings(tmp_path):
    cfg = yaml.safe_load((REPO / "config" / "settings.yml").read_text())
    for sub in ["data/source", "metadata", "state"]:
        (tmp_path / sub).mkdir(parents=True)
    make_synthetic(tmp_path / "data/source/ddos_traffic.csv")
    cfg["_root"] = str(tmp_path)
    cfg["ingestion"]["num_batches"] = 4
    return cfg


@pytest.fixture()
def ready(settings):
    from src import source
    source.write_manifest(settings)
    return settings
