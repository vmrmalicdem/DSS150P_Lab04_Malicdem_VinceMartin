# Engineering Gaps

**Project:** DDoS Traffic classification (`kaggle-oktayrdeki-ddos-traffic-dataset`)
**Source checksum (SHA-256):** `f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504`

No prior group project exists for this submission. The dataset was assigned directly, so there is no previous notebook to audit. The gaps below are (a) the risks of the notebook-style approach this lifecycle replaces and (b) data issues found during source profiling. The baseline in `metadata/expected_metrics.json` was therefore captured from a first documented from-scratch run.

| # | Current behavior | Risk | DSS150P concept | Remediation |
|---|---|---|---|---|
| 1 | Dataset read from a personal desktop path | Cannot run on another machine | Configured paths, dataset manifest | Relative paths in `config/settings.yml`; `source_manifest.yml` with SHA-256 |
| 2 | Random train/test split without a seed | Metrics change every run | Determinism | Fixed seed (42) in config; split manifest in Phase II |
| 3 | Notebook cells run out of order | Hidden execution state | Modular code, command entry point | Functions in `src/`; `python -m src.cli` and `python -m src.ingest` |
| 4 | Manual data cleaning | Not repeatable | Staging transformation | Cleaning code in a transform step (Phase II) |
| 5 | Model metrics only visible in a notebook | No machine-readable output | Persisted outputs | Write `metrics.json`; compare against `expected_metrics.json` |
| 6 | Packages unpinned | Environment-specific results | Reproducible environment | Pinned `requirements.txt` |
| 7 | No data validation | Bad data fails silently | Data contract | `data_contract.yml` with fail/quarantine/warn rules |
| 8 | `Source IP` is identical to the target (crosstab: 454,849 rows 0/0 and 397,736 rows 1/1, no mixed rows) | Perfect but meaningless metrics (first run scored 1.0 on every metric) | Leakage control | Exclude the column from modelling; auto-flagged as "leaks target" in the profile and listed in the contract |
| 9 | 89.9% of source rows are exact duplicates (766,840 of 852,585) | Identical rows in train and test inflate metrics | Data quality | Deduplicate before splitting; report both full-data and deduplicated results |
| 10 | Reruns re-append already loaded data | Duplicate records | Idempotent ingestion | Checkpoint file (state/ingest_checkpoint.json) plus a deterministic row hash; exact duplicates quarantined (210,437 in batch 1 alone); --force for full refresh. |
| 11 | A crash mid-load can leave partial batches | Partial or lost data | Recoverability | Batch written, validated, then committed to the checkpoint; orphan files cleaned on rerun; --stop-after N demonstrates recovery. |
| 12 | `Packets/Time` name matches a timestamp pattern | Wrongly used as a watermark column | Schema risk | Treated as a rate feature, not a timestamp; incremental simulation uses row sequence |

## Manual, hidden, non-deterministic or environment-specific behavior

- **Manual:** data cleaning and feature choices made by hand (gaps 4 and 8).
- **Hidden:** execution order and state in notebook-style work (gap 3).
- **Non-deterministic:** unseeded splits (gap 2).
- **Environment-specific:** local file paths and unpinned packages (gaps 1 and 6).
