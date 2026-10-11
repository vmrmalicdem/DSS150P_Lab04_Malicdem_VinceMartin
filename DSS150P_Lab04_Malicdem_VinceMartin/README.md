# DSS150P Reproduction Lab — DDoS Traffic Classification

Individual repository for DSS150P Integrated Individual Laboratory Activity #4 (reproducing a predictive analytics
project as a rerun-safe data product).

| Field | Entry |
|---|---|
| Name / Student number / Section | _fill in_ |
| Previous group / project | _fill in_ (Predictive Analytics) |
| Dataset | DDoS Traffic — https://www.kaggle.com/datasets/oktayrdeki/ddos-traffic-dataset |
| Predictive task | Classification (attack vs benign traffic; confirm label column in `metadata/source_profile.md`) |

**Status: Phase I complete (source control, contract, ingestion).** Transformation, storage formats, Docker/Compose,
Airflow DAG, quality checks, training/evaluation and reports are Phase II+ and not built yet. Rename this folder to
`DSS150P_REPRO_LASTNAME_FIRSTNAME` with your own name before pushing.

Prior group work is reused only for the problem statement, target and model family; cite the group project here:
_add citation_.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                      # only environment-specific values
# 1. Download the CSV from Kaggle and place it at data/source/ddos_traffic.csv (see data/source/README.md).
#    If the file name differs, set SOURCE_FILE in .env or dataset.source_file in config/settings.yml.
python -m src.cli manifest        # first time only: freezes checksum -> metadata/source_manifest.yml
python -m src.cli verify-source   # every other machine: must print OK
python -m src.cli profile         # metadata/source_profile.md + metadata/data_contract.yml
python -m src.cli ingest --mode full
python -m src.cli ingest          # incremental simulation (4 batches); run it twice to see batches skipped
python -m src.cli status
python -m pytest -q
```

Set `dataset.target_column` in `config/settings.yml` after reading the profile (the CLI auto-detects common names
such as `Label`), then rerun `profile` so the contract records the target. Commit the generated files in
`metadata/` (they contain the checksum, not the data).

## How ingestion works

- **Full ingest:** byte-for-byte copy of the source into `data/raw/<run_id>/full/` plus `_audit.json`
  (`pipeline_run_id`, `ingested_at_utc`, `source_file`, `source_checksum`). The copy is checksum-verified.
- **Incremental (laboratory simulation, not a natural business watermark):** rows get `_source_seq` (their position
  in the immutable file); the file is split into `ingestion.num_batches` (default 4, minimum 3) contiguous batches.
  The checkpoint in `state/ingest_checkpoint.json` records committed batches and the last committed sequence.
  Rerunning skips committed batches; `--force` discards the checkpoint and rebuilds.
- **Duplicates:** a deterministic hash of all source columns (or the configured `business_key`) identifies a logical
  record. Duplicates are written to `data/quarantine/dupes_batch_XXXX.csv`, not appended. Note this also removes
  genuinely identical rows in the source; the counts are in the checkpoint so nothing disappears silently.
- **Safety:** every ingest first checks the source against the manifest checksum and refuses on mismatch. Batch files
  are written to a temp name and renamed, and the checkpoint is updated only afterwards.
- **Audit columns** on every ingested row: `pipeline_run_id`, `ingested_at_utc`, `source_file`, `source_checksum`,
  `batch_id`. Per-run summaries go to `state/runs/<run_id>.json`.

## Layout and deviations

Follows the required structure. Deviations: `src/source.py` holds checksum/manifest/profile/contract logic (instead of
inside `ingest.py`), `state/` holds the checkpoint and run logs (kept out of Git), `scripts/` holds the diagram
generator, and `docs/engineering_gaps.md` holds the lifecycle map and gap table. Files for later phases
(`transform.py`, `storage.py`, `validate.py`, `train.py`, `evaluate.py`, SQL, DAG, Docker files, runbook, reports)
are intentionally absent for now.

## Tests

`pytest` uses a small synthetic stand-in generated in a temp folder, never the real dataset. They cover checksum
verification, profiling/contract output, byte-exact full ingest, ≥3 batches, rerun skipping, duplicate quarantine,
crash-resume, `--force`, and refusal when the source changes.

## Licensing

The raw dataset is third-party and is not committed (`.gitignore`). Credentials and `.env` are never committed.
