"""Tests for the PostgreSQL loading logic that do not need a database server.

The schema and the ON CONFLICT upsert are exercised on SQLite (same syntax for this subset);
COPY and the connection are covered by `python -m src.cli load-db` against the Compose database.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from src import db

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


def _curated_rows(n=5):
    return pd.DataFrame({
        "record_hash": [f"h{i:03d}" for i in range(n)],
        "source_row_key": np.arange(n, dtype="int64"),
        "highest_layer": ["TCP"] * n, "transport_layer": ["UDP"] * n, "dest_ip": ["192.168.1.1"] * n,
        "source_port": np.arange(n, dtype="int64"), "dest_port": [80] * n,
        "packet_length": [60] * n, "packets_per_time": np.linspace(1.5, 9.5, n),
        "target": [0, 1, 0, 1, 1][:n], "pipeline_run_id": ["run_a"] * n,
    })


def test_upsert_sql_text():
    sql = db.upsert_sql("t", ["k", "a", "b"], "k", "tmp_t")
    assert sql == ("INSERT INTO t (k, a, b) SELECT k, a, b FROM tmp_t WHERE TRUE "
                   "ON CONFLICT (k) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b")


def test_prepare_rows_returns_plain_python_values():
    df = _curated_rows(3)
    rows = db.prepare_rows(df, db.CURATED_COPY_COLS)
    assert len(rows) == 3 and len(rows[0]) == len(db.CURATED_COPY_COLS)
    assert all(type(v) in (int, float, str) for r in rows for v in r)


def test_prepare_rows_maps_nulls_to_none():
    df = pd.DataFrame({"a": pd.array([1, None], dtype="Int64"), "b": ["x", None]})
    assert db.prepare_rows(df, ["a", "b"]) == [(1, "x"), (None, None)]


def test_schema_runs_twice_and_upsert_is_rerun_safe():
    con = sqlite3.connect(":memory:")
    for _ in range(2):  # schema is idempotent
        con.executescript(SCHEMA.read_text())
    cols = db.CURATED_UPSERT_COLS
    frame = _curated_rows()
    frame["loaded_at_utc"] = "2026-01-01T00:00:00+00:00"

    def load(f):
        con.execute("DROP TABLE IF EXISTS tmp_curated_traffic")
        con.execute("CREATE TEMP TABLE tmp_curated_traffic AS SELECT * FROM curated_traffic WHERE 0")
        marks = ", ".join("?" * len(cols))
        con.executemany(f"INSERT INTO tmp_curated_traffic ({', '.join(cols)}) VALUES ({marks})",
                        db.prepare_rows(f, cols))
        con.execute(db.upsert_sql("curated_traffic", cols, "record_hash", "tmp_curated_traffic"))
        con.commit()

    load(frame)
    load(frame)  # rerun: no duplicates
    assert con.execute("SELECT count(*) FROM curated_traffic").fetchone()[0] == 5
    changed = frame.copy()
    changed.loc[0, "packet_length"] = 999
    load(changed)  # same key, new value: updated in place
    assert con.execute("SELECT count(*) FROM curated_traffic").fetchone()[0] == 5
    assert con.execute("SELECT packet_length FROM curated_traffic WHERE record_hash='h000'").fetchone()[0] == 999


def test_constraints_reject_bad_rows():
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA.read_text())
    ok = ("INSERT INTO curated_traffic (record_hash, source_row_key, highest_layer, transport_layer, dest_ip, "
          "source_port, dest_port, packet_length, packets_per_time, target, pipeline_run_id) "
          "VALUES (?, ?, 'TCP','UDP','1.1.1.1',1,1,60,1.0,?, 'r')")
    con.execute(ok, ("a", 1, 0))
    for args in (("a", 2, 0), ("b", 1, 0), ("c", 3, 7)):  # dup hash, dup row key, target not in (0,1)
        try:
            con.execute(ok, args)
            raise AssertionError(f"constraint did not fire for {args}")
        except sqlite3.IntegrityError:
            pass
