# Lifecycle Mapping and Engineering-Gap Table (Phase I)

Diagram: `docs/architecture.png` (regenerate with `python scripts/make_architecture.py`).

## Lifecycle map

| Stage | Previous group project (notebook) | This repository |
|---|---|---|
| Generation / source | Dataset downloaded manually from Kaggle | Frozen source, SHA-256 in `metadata/source_manifest.yml`, placement in `data/source/README.md` |
| Ingestion | `read_csv` from a local path inside notebook cells | `src/ingest.py`: byte-exact full copy and 4 deterministic batches with checkpoint |
| Storage | None (in-memory DataFrame) | `data/raw/` now; staging/curated, Parquet and PostgreSQL in later phases |
| Transformation | Interactive cleaning/encoding in cells | `src/transform.py` (Phase II) |
| Training / evaluation | Notebook cells, results read by eye | `src/train.py`, `src/evaluate.py` with `metrics.json` (later phases) |
| Outputs / consumer | Screenshots and report tables | `outputs/` files compared to `metadata/expected_metrics.json` |

> The previous notebook was not available while building Phase I, so the "previous project" column describes the
> typical notebook workflow and the gaps below are the expected ones. Replace entries with what your group actually
> did where it differs.

## Engineering-gap table

| Current behavior | Risk | DSS150P concept | Remediation |
|---|---|---|---|
| Dataset loaded from a personal local path | Cannot run on another machine | Source systems, reproducible environments | Relative, configured paths (`config/settings.yml`, `.env`); source manifest with checksum |
| Dataset identity assumed, not proven | Silent use of a different file/version | Data contracts | SHA-256 recorded and verified before every ingest (`verify-source`, ingest refuses on mismatch) |
| Random train/test split without a seed | Metrics change every run | Reproducibility | Fixed seed in config plus `split_manifest.json` (Phase II) |
| Notebook cells run out of order | Hidden execution state | Modular code, orchestration | Functions in `src/` with one CLI entry point |
| Manual data cleaning in cells | Not repeatable | ETL/ELT | Staging transformation code and SQL (Phase II) |
| Re-running ingestion appends again | Duplicate records | Ingestion patterns | Checkpointed batches, row-hash dedup, quarantine of duplicates, `--force` for full refresh |
| Crash mid-load leaves partial data | Corrupt or double-counted batches | Rerun-safe pipelines | Temp-file + atomic rename, checkpoint written only after the batch file exists |
| Metrics only visible in the notebook | No machine-readable record | Quality / observability | `metrics.json` plus tolerance comparison against `expected_metrics.json` (later phases) |
| Unpinned library versions | Different results on another machine | Reproducible environments | Pinned `requirements.txt`; Dockerfile and Compose (Phase II) |
| No audit trail | Cannot trace which run produced what | Lineage / operations | `pipeline_run_id`, `ingested_at_utc`, `source_file`, `source_checksum`, `batch_id` on every ingested row; per-run JSON logs in `state/runs/` |
