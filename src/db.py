"""PostgreSQL storage: schema creation and rerun-safe (idempotent) loading.

Loading pattern: COPY the rows into a temporary table, then
INSERT ... ON CONFLICT (record_hash) DO UPDATE. Running the load twice therefore leaves the
same rows in the tables (no duplicates), and the whole load is one transaction.

psycopg is imported lazily so the rest of the pipeline works without it installed.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd

from .config import resolve
from .ingest import new_run_id
from .transform import read_curated, read_staging, rejects_path

log = logging.getLogger("db")

COLUMN_MAP = {
    "Highest Layer": "highest_layer",
    "Transport Layer": "transport_layer",
    "Source IP": "source_ip",
    "Dest IP": "dest_ip",
    "Source Port": "source_port",
    "Dest Port": "dest_port",
    "Packet Length": "packet_length",
    "Packets/Time": "packets_per_time",
}

STAGING_COLS = [
    "record_hash", "source_row_key", "batch_id", "highest_layer", "transport_layer", "source_ip",
    "dest_ip", "source_port", "dest_port", "packet_length", "packets_per_time", "target",
    "pipeline_run_id", "processed_at_utc",
]
CURATED_COPY_COLS = [
    "record_hash", "source_row_key", "highest_layer", "transport_layer", "dest_ip", "source_port",
    "dest_port", "packet_length", "packets_per_time", "target", "pipeline_run_id",
]
CURATED_UPSERT_COLS = CURATED_COPY_COLS + ["loaded_at_utc"]  # loaded_at_utc comes from the column default
QUARANTINE_COLS = ["source_row_key", "reject_reason", "row_payload", "pipeline_run_id", "processed_at_utc"]


class DatabaseError(RuntimeError):
    """Connection, schema or load problem with PostgreSQL."""


# ---------------------------------------------------------------- pure helpers (unit-tested)
def upsert_sql(table: str, cols: list[str], key: str, source: str) -> str:
    """INSERT ... SELECT ... ON CONFLICT DO UPDATE statement moving rows from `source` into `table`."""
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != key)
    return (
        f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {source} WHERE TRUE "
        f"ON CONFLICT ({key}) DO UPDATE SET {updates}"
    )


def prepare_rows(df: pd.DataFrame, cols: list[str]) -> list[tuple]:
    """Convert a DataFrame slice into tuples of plain Python values (None for nulls)."""
    part = df[cols].astype(object)
    part = part.where(df[cols].notna(), None)
    return [tuple(r) for r in part.itertuples(index=False, name=None)]


def staging_frame(settings: dict) -> pd.DataFrame:
    df = read_staging(settings).rename(columns=COLUMN_MAP)
    if not df["record_hash"].is_unique:
        raise DatabaseError("record_hash is not unique in staging; refusing to load")
    return df[STAGING_COLS]


def curated_frame(settings: dict, run_id: str) -> pd.DataFrame:
    df = read_curated(settings).rename(columns=COLUMN_MAP)
    if not df["record_hash"].is_unique:
        raise DatabaseError("record_hash is not unique in curated data; refusing to load")
    df["pipeline_run_id"] = run_id
    return df[CURATED_COPY_COLS]


def quarantine_frame(settings: dict) -> pd.DataFrame:
    from psycopg.types.json import Jsonb

    raw = pd.read_csv(rejects_path(settings), dtype=str, keep_default_na=False)
    if raw.empty:
        return pd.DataFrame(columns=QUARANTINE_COLS)
    meta = {"reject_reason", "pipeline_run_id", "processed_at_utc"}
    payload_cols = [c for c in raw.columns if c not in meta]
    out = pd.DataFrame({
        "source_row_key": pd.to_numeric(raw["_source_seq"]).astype("int64"),
        "reject_reason": raw["reject_reason"],
        "row_payload": [Jsonb(r) for r in raw[payload_cols].to_dict(orient="records")],
        "pipeline_run_id": raw["pipeline_run_id"],
        "processed_at_utc": raw["processed_at_utc"],
    })
    return out


# ---------------------------------------------------------------- connection
def _db_params() -> dict:
    env = os.environ
    missing = [k for k in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB") if not env.get(k)]
    if missing:
        raise DatabaseError(f"Missing environment variables: {missing}. Copy .env.example to .env and edit it.")
    return {
        "host": env.get("POSTGRES_HOST", "localhost"),
        "port": int(env.get("POSTGRES_PORT", "5432")),
        "dbname": env["POSTGRES_DB"],
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
    }


def connect(timeout_s: int = 60):
    """Connect with retries, as a second safety net on top of the Compose health check."""
    import psycopg

    params = _db_params()
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return psycopg.connect(**params, connect_timeout=5)
        except psycopg.OperationalError as exc:
            if time.monotonic() >= deadline:
                raise DatabaseError(
                    f"PostgreSQL not reachable at {params['host']}:{params['port']} after {timeout_s}s: {exc}"
                ) from exc
            log.info("waiting for PostgreSQL at %s:%s ...", params["host"], params["port"])
            time.sleep(2)


def schema_text(settings: dict) -> str:
    return resolve(settings, "sql/schema.sql").read_text(encoding="utf-8")


# ---------------------------------------------------------------- loading
def _load_frame(cur, table: str, frame: pd.DataFrame, copy_cols: list[str], upsert_cols: list[str], key: str) -> int:
    if frame.empty:
        return 0
    tmp = f"tmp_{table}"
    cur.execute(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
    with cur.copy(f"COPY {tmp} ({', '.join(copy_cols)}) FROM STDIN") as cp:
        for row in prepare_rows(frame, copy_cols):
            cp.write_row(row)
    cur.execute(upsert_sql(table, upsert_cols, key, tmp))
    return cur.rowcount


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM {table}")
    return int(cur.fetchone()[0])


def load_all(settings: dict, run_id: str | None = None) -> dict:
    """Create the schema if needed and upsert staging, curated and quarantine data (one transaction)."""
    import psycopg

    run_id = run_id or new_run_id()
    staging, curated, quarantine = staging_frame(settings), curated_frame(settings, run_id), quarantine_frame(settings)
    expected = {"staging_traffic": len(staging), "curated_traffic": len(curated), "quarantine_traffic": len(quarantine)}
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(schema_text(settings))
            _load_frame(cur, "staging_traffic", staging, STAGING_COLS, STAGING_COLS, "record_hash")
            _load_frame(cur, "curated_traffic", curated, CURATED_COPY_COLS, CURATED_UPSERT_COLS, "record_hash")
            _load_frame(cur, "quarantine_traffic", quarantine, QUARANTINE_COLS, QUARANTINE_COLS, "source_row_key")
            counts = {t: _count(cur, t) for t in expected}
            stale = {t: counts[t] - n for t, n in expected.items() if counts[t] != n}
            if stale:
                raise DatabaseError(
                    f"Row counts differ from the files after loading (db minus file): {stale}. "
                    "The database holds rows from a different snapshot; reset the volume or use a fresh database."
                )
    except psycopg.Error as exc:
        raise DatabaseError(f"PostgreSQL error: {exc}") from exc
    log.info("loaded into PostgreSQL: %s", counts)
    return {"pipeline_run_id": run_id, "row_counts": counts}


def status(settings: dict) -> dict:
    import psycopg

    try:
        with connect() as conn, conn.cursor() as cur:
            out = {t: _count(cur, t) for t in ("staging_traffic", "curated_traffic", "quarantine_traffic")}
            cur.execute("SELECT count(DISTINCT record_hash) FROM curated_traffic")
            out["curated_distinct_record_hash"] = int(cur.fetchone()[0])
    except psycopg.Error as exc:
        raise DatabaseError(f"PostgreSQL error: {exc}") from exc
    return out
