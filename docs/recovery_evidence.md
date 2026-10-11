# Recovery evidence

Generated 2026-10-11T15:14:31+00:00 by `scripts/recovery_demo.py`.

## 1. Clean start (full refresh), then stop on purpose after 2 batches

`python -m src.cli ingest --force --stop-after 2` → exit code 3 (as expected)

```text
STOPPED: Stopped on purpose after 2 committed batch(es) (--stop-after). Rerun without the flag to resume.
2026-10-11 23:14:32,409 WARNING ingest: --force: clearing checkpoint and raw batches
2026-10-11 23:14:34,057 INFO ingest: batch 1 committed: 2710 written, 210437 quarantined
2026-10-11 23:14:35,357 INFO ingest: batch 2 committed: 962 written, 212185 quarantined
```

## 2. Show checkpoint after the interruption (only batches 1-2 committed)

`python -m src.cli status` → exit code 0 (as expected)

```text
{
  "source_checksum": "f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504",
  "total_rows": 852585,
  "num_batches": 4,
  "batch_size": 213147,
  "strategy": "row_sequence_simulation",
  "last_committed_seq": 426293,
  "committed_batches": [
    {
      "batch_id": 1,
      "seq_start": 0,
      "seq_end": 213146,
      "rows_read": 213147,
      "rows_written": 2710,
      "duplicates_quarantined": 210437,
      "file": "data\\raw\\batches\\batch_0001.csv",
      "pipeline_run_id": "run_20261011T151432Z_04e2b8",
      "committed_at_utc": "2026-10-11T15:14:34+00:00"
    },
    {
      "batch_id": 2,
      "seq_start": 213147,
      "seq_end": 426293,
      "rows_read": 213147,
      "rows_written": 962,
      "duplicates_quarantined": 212185,
      "file": "data\\raw\\batches\\batch_0002.csv",
      "pipeline_run_id": "run_20261011T151432Z_04e2b8",
      "committed_at_utc": "2026-10-11T15:14:35+00:00"
    }
  ]
}
raw rows=3672 distinct_row_hashes=3672 duplicates=0
```

## 3. Resume: rerun without the flag (batches 1-2 skipped, 3+ written)

`python -m src.cli ingest` → exit code 0 (as expected)

```text
{
  "pipeline_run_id": "run_20261011T151436Z_45bcc3",
  "mode": "incremental",
  "status": "completed",
  "finished_at_utc": "2026-10-11T15:14:40+00:00",
  "source_checksum": "f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504",
  "batches_total": 4,
  "batches_written": [
    3,
    4
  ],
  "batches_skipped": [
    1,
    2
  ],
  "uncommitted_leftovers_removed": [],
  "last_committed_seq": 852584,
  "rows_written_this_run": 82073
}
2026-10-11 23:14:39,226 INFO ingest: batch 3 committed: 76820 written, 136327 quarantined
2026-10-11 23:14:40,577 INFO ingest: batch 4 committed: 5253 written, 207891 quarantined
```

## 4. Show final state (no logical duplicates)

`python -m src.cli status` → exit code 0 (as expected)

```text
{
  "source_checksum": "f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504",
  "total_rows": 852585,
  "num_batches": 4,
  "batch_size": 213147,
  "strategy": "row_sequence_simulation",
  "last_committed_seq": 852584,
  "committed_batches": [
    {
      "batch_id": 1,
      "seq_start": 0,
      "seq_end": 213146,
      "rows_read": 213147,
      "rows_written": 2710,
      "duplicates_quarantined": 210437,
      "file": "data\\raw\\batches\\batch_0001.csv",
      "pipeline_run_id": "run_20261011T151432Z_04e2b8",
      "committed_at_utc": "2026-10-11T15:14:34+00:00"
    },
    {
      "batch_id": 2,
      "seq_start": 213147,
      "seq_end": 426293,
      "rows_read": 213147,
      "rows_written": 962,
      "duplicates_quarantined": 212185,
      "file": "data\\raw\\batches\\batch_0002.csv",
      "pipeline_run_id": "run_20261011T151432Z_04e2b8",
      "committed_at_utc": "2026-10-11T15:14:35+00:00"
    },
    {
      "batch_id": 3,
      "seq_start": 426294,
      "seq_end": 639440,
      "rows_read": 213147,
      "rows_written": 76820,
      "duplicates_quarantined": 136327,
      "file": "data\\raw\\batches\\batch_0003.csv",
      "pipeline_run_id": "run_20261011T151436Z_45bcc3",
      "committed_at_utc": "2026-10-11T15:14:39+00:00"
    },
    {
      "batch_id": 4,
      "seq_start": 639441,
      "seq_end": 852584,
      "rows_read": 213144,
      "rows_written": 5253,
      "duplicates_quarantined": 207891,
      "file": "data\\raw\\batches\\batch_0004.csv",
      "pipeline_run_id": "run_20261011T151436Z_45bcc3",
      "committed_at_utc": "2026-10-11T15:14:40+00:00"
    }
  ]
}
raw rows=85745 distinct_row_hashes=85745 duplicates=0
```

## 5. Rerun again (everything skipped, no new rows)

`python -m src.cli ingest` → exit code 0 (as expected)

```text
{
  "pipeline_run_id": "run_20261011T151442Z_381ee0",
  "mode": "incremental",
  "status": "completed",
  "finished_at_utc": "2026-10-11T15:14:43+00:00",
  "source_checksum": "f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504",
  "batches_total": 4,
  "batches_written": [],
  "batches_skipped": [
    1,
    2,
    3,
    4
  ],
  "uncommitted_leftovers_removed": [],
  "last_committed_seq": 852584,
  "rows_written_this_run": 0
}
```

## Result

PASS: the interrupted run resumed from the checkpoint without duplicating batches.
