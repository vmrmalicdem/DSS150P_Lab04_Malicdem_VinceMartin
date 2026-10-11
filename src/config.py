"""Configuration loading. All paths are relative to the repository root."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env(env_file: Path | None = None) -> None:
    """Load KEY=VALUE pairs from .env into os.environ (without overriding real env vars)."""
    env_file = env_file or REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_settings(settings_path: str | os.PathLike | None = None) -> dict:
    load_env()
    path = Path(settings_path or os.environ.get("PIPELINE_SETTINGS", "config/settings.yml"))
    if not path.is_absolute():
        path = REPO_ROOT / path
    with open(path, encoding="utf-8") as fh:
        settings = yaml.safe_load(fh)
    settings["_root"] = str(REPO_ROOT)
    override = os.environ.get("SOURCE_FILE")
    if override:
        settings["dataset"]["source_file"] = override
    return settings
    seed = os.environ.get("RANDOM_SEED")
    if seed:
        settings["reproducibility"]["random_seed"] = int(seed)


def resolve(settings: dict, relative: str) -> Path:
    """Resolve a settings path against the repo root (absolute paths are rejected)."""
    p = Path(relative)
    if p.is_absolute():
        raise ValueError(f"Absolute paths are not allowed in configuration: {relative}")
    return Path(settings["_root"]) / p
