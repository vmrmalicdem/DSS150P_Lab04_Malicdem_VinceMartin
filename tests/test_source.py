import yaml

from src import source
from src.config import resolve


def test_manifest_checksum_verifies(ready):
    ok, exp, act = source.verify_source(ready)
    assert ok and exp == act and len(exp) == 64


def test_checksum_detects_change(ready):
    p = source.source_path(ready)
    p.write_text(p.read_text() + "1,1,1.0,6,DDoS\n")
    ok, _, _ = source.verify_source(ready)
    assert not ok


def test_profile_and_contract(ready):
    prof = source.profile_source(ready)
    assert prof["rows"] == 106 and prof["duplicate_rows"] >= 3
    assert prof["target"] == "Label"
    assert set(prof["target_distribution"]) == {"BENIGN", "DDoS"}
    md = source.write_profile_md(ready, prof)
    assert "SHA-256" in md.read_text()
    c = yaml.safe_load(source.write_contract(ready, prof).read_text())
    assert c["target"] == "Label"
    assert {col["name"] for col in c["required_columns"]} == {
        "Flow Duration", "Total Fwd Packets", "Packet Length Mean", "Protocol", "Label"}
    assert c["on_violation"]["missing_required_column"] == "fail"


def test_no_absolute_paths_in_config():
    import re
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    cfg = (root / "config/settings.yml").read_text()
    assert not re.search(r"[A-Za-z]:\\\\|/Users/|/home/", cfg)
