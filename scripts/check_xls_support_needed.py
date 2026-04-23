#!/usr/bin/env python3
"""Print whether the current config requires xlrd/.xls support."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import config as core_config
from scripts.core import health_checks as core_health_checks


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config.yaml"

def config_needs_xls_support(config: dict) -> bool:
    raw_data_dir = core_config.resolve_system_raw_data_dir(config, BASE_DIR)
    return core_health_checks.needs_xls_support(config, raw_data_dir)


def main() -> int:
    try:
        config = core_config.load_config(CONFIG_PATH) if CONFIG_PATH.exists() else {}
    except Exception:
        print("no")
        return 0
    print("yes" if config_needs_xls_support(config) else "no")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
