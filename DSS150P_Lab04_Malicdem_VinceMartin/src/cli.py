"""Command entry point: python -m src.cli <command>"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import ingest, source
from .config import load_settings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m src.cli", description="DSS150P reproducible pipeline")
    ap.add_argument("--settings", default=None, help="Path to settings YAML (default config/settings.yml)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("manifest", help="Record SHA-256 + metadata of the source in metadata/source_manifest.yml")
    sub.add_parser("verify-source", help="Check the source file against the manifest checksum")
    sub.add_parser("profile", help="Write source_profile.md and data_contract.yml from the source")
    p_ing = sub.add_parser("ingest", help="Ingest the source into the raw layer")
    p_ing.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p_ing.add_argument("--force", action="store_true", help="Full refresh: discard checkpoint and batches")
    sub.add_parser("status", help="Show checkpoint state and logical duplicate check")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings(args.settings)

    try:
        if args.cmd == "manifest":
            m = source.write_manifest(settings)
            print(f"sha256 {m['checksum']['value']}  ({m['size_bytes']:,} bytes)")
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
                res = ingest.incremental_ingest(settings, force=args.force)
            print(json.dumps(res, indent=2))
        elif args.cmd == "status":
            print(json.dumps(ingest.load_checkpoint(settings), indent=2))
            rows, distinct = ingest.logical_row_count(settings)
            print(f"raw rows={rows} distinct_row_hashes={distinct} duplicates={rows - distinct}")
    except (FileNotFoundError, ingest.SourceChangedError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
