import json

import pytest

from src import ingest, source
from src.config import resolve


def test_full_ingest_copies_bytes_unchanged(ready):
    before = source.sha256_file(source.source_path(ready))
    audit = ingest.full_ingest(ready, run_id="run_test")
    raw = resolve(ready, audit["raw_file"])
    assert source.sha256_file(raw) == before
    assert source.sha256_file(source.source_path(ready)) == before
    for k in ["pipeline_run_id", "ingested_at_utc", "source_file", "source_checksum"]:
        assert audit[k]


def test_incremental_creates_at_least_three_batches(ready):
    res = ingest.incremental_ingest(ready, run_id="r1")
    assert len(res["batches_written"]) >= 3
    cp = ingest.load_checkpoint(ready)
    assert cp["last_committed_seq"] == cp["total_rows"] - 1
    assert [b["seq_start"] for b in cp["committed_batches"]] == sorted(b["seq_start"] for b in cp["committed_batches"])


def test_rerun_skips_committed_batches_and_adds_no_rows(ready):
    ingest.incremental_ingest(ready, run_id="r1")
    rows1, _ = ingest.logical_row_count(ready)
    res2 = ingest.incremental_ingest(ready, run_id="r2")
    rows2, distinct = ingest.logical_row_count(ready)
    assert res2["batches_written"] == [] and len(res2["batches_skipped"]) == 4
    assert rows1 == rows2 and rows2 == distinct  # no logical duplicates


def test_duplicates_are_quarantined_not_appended(ready):
    ingest.incremental_ingest(ready, run_id="r1")
    cp = ingest.load_checkpoint(ready)
    assert sum(b["duplicates_quarantined"] for b in cp["committed_batches"]) >= 3
    assert sum(b["rows_written"] for b in cp["committed_batches"]) + \
        sum(b["duplicates_quarantined"] for b in cp["committed_batches"]) == cp["total_rows"]


def test_resume_after_partial_run(ready):
    ingest.incremental_ingest(ready, run_id="r1")
    cpath = resolve(ready, ready["paths"]["checkpoint_file"])
    cp = json.loads(cpath.read_text())
    cp["committed_batches"] = cp["committed_batches"][:2]  # simulate crash after batch 2
    cp["last_committed_seq"] = cp["committed_batches"][-1]["seq_end"]
    cpath.write_text(json.dumps(cp))
    # batch files 3-4 still exist on disk as orphans (written, never committed) -> rerun must replace them,
    # not treat their rows as duplicates of themselves
    res = ingest.incremental_ingest(ready, run_id="r2")
    assert res["batches_written"] == [3, 4] and res["batches_skipped"] == [1, 2]
    assert sorted(res["uncommitted_leftovers_removed"]) == ["batch_0003.csv", "batch_0004.csv"]
    rows, distinct = ingest.logical_row_count(ready)
    cp2 = ingest.load_checkpoint(ready)
    assert rows == distinct == sum(b["rows_written"] for b in cp2["committed_batches"])
    assert sum(b["duplicates_quarantined"] for b in cp2["committed_batches"]) == 3  # only the real duplicates


def test_stop_after_then_resume_without_duplicates(ready):
    with pytest.raises(ingest.IntentionalStop):
        ingest.incremental_ingest(ready, run_id="r1", stop_after=2)
    cp = ingest.load_checkpoint(ready)
    assert [b["batch_id"] for b in cp["committed_batches"]] == [1, 2]
    rows_mid, _ = ingest.logical_row_count(ready)
    res = ingest.incremental_ingest(ready, run_id="r2")
    assert res["batches_skipped"] == [1, 2] and res["batches_written"] == [3, 4]
    rows, distinct = ingest.logical_row_count(ready)
    assert rows == distinct and rows > rows_mid
    again = ingest.incremental_ingest(ready, run_id="r3")
    assert again["batches_written"] == [] and ingest.logical_row_count(ready) == (rows, distinct)


def test_failed_validation_leaves_checkpoint_unchanged(ready, monkeypatch):
    real = ingest._validate_batch
    calls = {"n": 0}

    def flaky(path, expected, hash_col, s, e):
        calls["n"] += 1
        if calls["n"] == 3:  # third batch fails validation
            raise ingest.BatchValidationError("injected failure")
        return real(path, expected, hash_col, s, e)

    monkeypatch.setattr(ingest, "_validate_batch", flaky)
    with pytest.raises(ingest.BatchValidationError):
        ingest.incremental_ingest(ready, run_id="r1")
    cp = ingest.load_checkpoint(ready)
    assert [b["batch_id"] for b in cp["committed_batches"]] == [1, 2]
    assert not (resolve(ready, ready["paths"]["raw_dir"]) / "batches" / "batch_0003.csv").exists()
    monkeypatch.setattr(ingest, "_validate_batch", real)
    res = ingest.incremental_ingest(ready, run_id="r2")
    assert res["batches_written"] == [3, 4]
    rows, distinct = ingest.logical_row_count(ready)
    assert rows == distinct


def test_validator_catches_corrupted_batch(ready, tmp_path):
    import pandas as pd
    df = pd.DataFrame({"_row_hash": ["a", "b"], "_source_seq": [0, 1], "pipeline_run_id": ["r", "r"],
                       "ingested_at_utc": ["t", "t"], "source_file": ["f", "f"], "source_checksum": ["c", "c"],
                       "batch_id": [1, 1]})
    p = tmp_path / "b.csv"
    df.to_csv(p, index=False)
    ingest._validate_batch(p, df, "_row_hash", 0, 1)  # good file passes
    df.iloc[:1].to_csv(p, index=False)  # truncated on disk
    with pytest.raises(ingest.BatchValidationError):
        ingest._validate_batch(p, df, "_row_hash", 0, 1)


def test_force_rebuilds_from_scratch(ready):
    ingest.incremental_ingest(ready, run_id="r1")
    res = ingest.incremental_ingest(ready, run_id="r2", force=True)
    assert len(res["batches_written"]) == 4 and res["batches_skipped"] == []
    rows, distinct = ingest.logical_row_count(ready)
    assert rows == distinct


def test_changed_source_is_refused(ready):
    p = source.source_path(ready)
    p.write_text(p.read_text() + "9,9,9.0,6,DDoS\n")
    with pytest.raises(ingest.SourceChangedError):
        ingest.incremental_ingest(ready, run_id="r1")
