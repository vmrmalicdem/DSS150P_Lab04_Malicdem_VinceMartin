"""Command entry point: python -m src.cli <command>"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import db, ingest, model, source, transform
from .config import load_settings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m src.cli", description="DSS150P reproducible pipeline")
    ap.add_argument("--settings", default=None, help="Path to settings YAML (default config/settings.yml)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("manifest", help="Record SHA-256 + metadata of the source in metadata/source_manifest.yml")
    sub.add_parser("verify-source", help="Check the source file against the manifest checksum")
    sub.add_parser("profile", help="Write source_profile.md and data_contract.yml from the source")
    p_ing = sub.add_parser("ingest", help="Ingest the source into the rawlayer")
    p_ing.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p_ing.add_argument("--force", action="store_true", help="Full refresh: discard checkpoint and batches")
    p_ing.add_argument("--stop-after", type=int, default=None, metavar="N",
                       help="Recovery demo: stop on purpose after N committed batches (exit code 3)")
    sub.add_parser("status", help="Show checkpoint state and logical duplicate check")
    sub.add_parser("stage", help="Raw -> staging: typing, validation, quarantine, audit columns")
    sub.add_parser("curate", help="Staging -> curated: model columns, leakage exclusions, deterministic order")
    sub.add_parser("train", help="Curated -> split manifest, trained model, outputs/metrics.json")
    sub.add_parser("compare", help="Compare outputs/metrics.json with metadata/expected_metrics.json (exit 4 on failure)")
    sub.add_parser("pipeline", help="stage -> curate -> train -> compare")
    sub.add_parser("load-db", help="Create schema and upsert staging/curated/quarantine into PostgreSQL (rerun-safe)")
    sub.add_parser("db-status", help="Show PostgreSQL table row counts")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings(args.settings)

    try:
        if args.cmd == "manifest":
            m = source.write_manifest(settings)
            print(f"sha256 {m['checksum']['value']}  ({m['size_bytes']:,}bytes)")
        elif args.cmd == "verify-source":
            ok, expected, actual = source.verify_source(settings)
            print("OK: checksum matches manifest" if ok else f"MISMATCH expected={expected} actual={actual}")
            return 0 if ok else 2
        elif args.cmd == "profile":
            prof = source.profile_source(settings)
            print("wrote", source.write_profile_md(settings, prof))
            print("wrote", source.write_contract(settings, prof))
        elif args.cmd == "ingest":
            if args.mode == "full":
                res = ingest.full_ingest(settings)
            else:
                res = ingest.incremental_ingest(settings, force=args.force, stop_after=args.stop_after)
            print(json.dumps(res, indent=2))
        elif args.cmd == "status":
            print(json.dumps(ingest.load_checkpoint(settings), indent=2))
            rows, distinct = ingest.logical_row_count(settings)
            print(f"raw rows={rows} distinct_row_hashes={distinct} duplicates={rows - distinct}")
        elif args.cmd == "stage":
            print(json.dumps(transform.build_staging(settings), indent=2))
        elif args.cmd == "curate":
            print(json.dumps(transform.build_curated(settings), indent=2))
        elif args.cmd == "train":
            res = model.run_training(settings)
            print(json.dumps({"pipeline_run_id": res["pipeline_run_id"], "metrics": res["metrics"]}, indent=2))
        elif args.cmd == "compare":
            print(json.dumps(model.compare_metrics(settings), indent=2))
        elif args.cmd == "pipeline":
            transform.build_staging(settings)
            transform.build_curated(settings)
            model.run_training(settings)
            rep = model.compare_metrics(settings)
            print(f"pipeline OK; baseline comparison passed={rep['passed']}")
        elif args.cmd == "load-db":
            print(json.dumps(db.load_all(settings), indent=2))
        elif args.cmd == "db-status":
            print(json.dumps(db.status(settings), indent=2))
    except ingest.IntentionalStop as exc:
        print(f"STOPPED: {exc}")
        return 3
    except model.ComparisonFailed as exc:
        print(f"COMPARISON FAILED: {exc}", file=sys.stderr)
        return 4
    except (FileNotFoundError, ingest.SourceChangedError, ingest.BatchValidationError,
            transform.ContractError, db.DatabaseError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
