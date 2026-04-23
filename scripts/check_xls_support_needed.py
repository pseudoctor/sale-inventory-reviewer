#!/usr/bin/env python3
"""Print whether the current config requires xlrd/.xls support."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import config as core_config


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config.yaml"


def is_xls_file(name: object) -> bool:
    return isinstance(name, str) and name.strip().lower().endswith(".xls")


def config_needs_xls_support(config: dict) -> bool:
    if is_xls_file(config.get("inventory_file")) or is_xls_file(config.get("carton_factor_file")):
        return True
    if any(is_xls_file(name) for name in config.get("sales_files", [])):
        return True

    if str(config.get("run_mode", "single")).lower() != "batch":
        return False

    for system in config.get("batch", {}).get("systems", []):
        if not bool(system.get("enabled", True)):
            continue
        if is_xls_file(system.get("inventory_file")) or is_xls_file(system.get("carton_factor_file")):
            return True
        if any(is_xls_file(name) for name in system.get("sales_files", [])):
            return True
    return False


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
