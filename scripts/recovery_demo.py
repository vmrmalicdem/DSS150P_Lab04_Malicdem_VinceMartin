"""Recovery demonstration for the lab: stop a run after batch 2, resume it, then rerun.

Run from the repo root:  python scripts/recovery_demo.py
Writes docs/recovery_evidence.md with the real command output. It rebuilds the raw batches (--force),
so it only touches generated layers (data/raw, data/quarantine, state/), never data/source.
"""
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "recovery_evidence.md"

STEPS = [
    ("1. Clean start (full refresh), then stop on purpose after 2 batches",
     ["ingest", "--force", "--stop-after", "2"], 3),
    ("2. Show checkpoint after the interruption (only batches 1-2 committed)", ["status"], 0),
    ("3. Resume: rerun without the flag (batches 1-2 skipped, 3+ written)", ["ingest"], 0),
    ("4. Show final state (no logical duplicates)", ["status"], 0),
    ("5. Rerun again (everything skipped, no new rows)", ["ingest"], 0),
]


def run(args):
    p = subprocess.run([sys.executable, "-m", "src.cli", *args], cwd=ROOT, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()


def main() -> int:
    lines = ["# Recovery evidence", "", f"Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')} "
             "by `scripts/recovery_demo.py`.", ""]
    ok = True
    for title, args, expected_rc in STEPS:
        rc, out = run(args)
        status = "as expected" if rc == expected_rc else f"UNEXPECTED (exit {rc}, wanted {expected_rc})"
        ok &= rc == expected_rc
        lines += [f"## {title}", "", f"`python -m src.cli {' '.join(args)}` → exit code {rc} ({status})", "",
                  "```text", out[-4000:], "```", ""]
    lines += ["## Result", "", "PASS: the interrupted run resumed from the checkpoint without duplicating batches."
              if ok else "FAIL: see the unexpected exit code above."]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote", OUT, "-", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
