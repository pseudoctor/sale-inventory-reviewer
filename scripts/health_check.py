#!/usr/bin/env python3
"""Runtime health check for inventory risk report pipeline."""

from __future__ import annotations

from pathlib import Path
import importlib
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import config as core_config
from scripts.core import io as core_io

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config.yaml"


def _ok(msg: str) -> None:
    print(f"[PASS] {msg}")


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def _check_python() -> list[str]:
    errors: list[str] = []
    if sys.version_info < (3, 9):
        errors.append(f"Python >= 3.9 required, current: {sys.version.split()[0]}")
    return errors


def _check_dependencies() -> list[str]:
    errors: list[str] = []
    for pkg in ("pandas", "openpyxl", "yaml", "xlrd"):
        try:
            module = importlib.import_module(pkg)
            if pkg == "yaml":
                safe_load = getattr(module, "safe_load", None)
                if not callable(safe_load):
                    module_path = getattr(module, "__file__", None)
                    errors.append(
                        "broken dependency 'yaml': missing callable safe_load "
                        f"(module_path={module_path!r}). Rebuild venv and reinstall requirements.lock."
                    )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"missing dependency '{pkg}': {exc}")
    return errors


def _check_config_and_paths() -> list[str]:
    errors: list[str] = []
    try:
        config = core_config.load_config(CONFIG_PATH)
    except Exception as exc:  # noqa: BLE001
        return [f"invalid config.yaml: {exc}"]

    reports_dir = BASE_DIR / "reports"
    try:
        reports_dir.mkdir(parents=True, exist_ok=True)
        probe = reports_dir / ".health_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"reports directory is not writable: {exc}")

    raw_data_dir = Path(config["raw_data_dir"])
    if not raw_data_dir.is_absolute():
        raw_data_dir = (BASE_DIR / raw_data_dir).resolve()
    if not raw_data_dir.exists():
        errors.append(f"raw_data_dir not found: {raw_data_dir}")

    configured_brand_keywords = [str(x).strip() for x in (config.get("brand_keywords") or []) if str(x).strip()]
    if not configured_brand_keywords:
        errors.append("brand_keywords is empty. Configure at least one brand keyword in config.yaml.")

    run_mode = str(config.get("run_mode", "single")).lower()
    if run_mode == "batch":
        try:
            core_config.validate_batch_config(config, BASE_DIR)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"batch config validation failed: {exc}")
            return errors

        for idx, system in enumerate(config["batch"].get("systems", []), start=1):
            if not bool(system.get("enabled", True)):
                continue
            try:
                merged = core_config.build_system_config(system, config)
                merged_brand_keywords = [str(x).strip() for x in (merged.get("brand_keywords") or []) if str(x).strip()]
                if not merged_brand_keywords:
                    errors.append(
                        f"batch.systems[{idx}] '{merged['display_name']}' brand_keywords is empty."
                    )
                system_raw = core_config.resolve_system_raw_data_dir(merged, BASE_DIR)
                sales_files = [system_raw / name for name in merged.get("sales_files", [])]
                missing_sales = [str(p.name) for p in sales_files if not p.exists()]
                if missing_sales:
                    errors.append(
                        f"batch.systems[{idx}] '{merged['display_name']}' missing sales files: {', '.join(missing_sales)}"
                    )
                inv = system_raw / merged["inventory_file"]
                if not inv.exists():
                    errors.append(
                        f"batch.systems[{idx}] '{merged['display_name']}' missing inventory file: {inv.name}"
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"batch.systems[{idx}] preflight failed: {exc}")
    else:
        inv = raw_data_dir / str(config.get("inventory_file", ""))
        if not inv.exists():
            errors.append(f"inventory file not found: {inv}")
        configured_sales = config.get("sales_files", [])
        sales = [raw_data_dir / name for name in configured_sales]
        missing_sales = [str(p.name) for p in sales if not p.exists()]
        if missing_sales:
            errors.append(f"missing sales files: {', '.join(missing_sales)}")
        if not configured_sales:
            try:
                candidates = core_io.resolve_sales_candidates(raw_data_dir, [])
                if not candidates:
                    ignored = core_io.list_ignored_sales_files(raw_data_dir, [])
                    ignored_text = core_io.format_ignored_sales_files(ignored)
                    suffix = f" Ignored files: {ignored_text}" if ignored_text else ""
                    errors.append(
                        "no auto-detected sales files in single mode "
                        "(expected sales keyword + YYYYMM)." + suffix
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"sales auto-scan preflight failed: {exc}")

    return errors


def main() -> int:
    failures = 0
    python_errors = _check_python()
    if python_errors:
        failures += len(python_errors)
        _fail("Python version")
        for err in python_errors:
            print(f"  - {err}")
    else:
        _ok("Python version")

    dependency_errors = _check_dependencies()
    if dependency_errors:
        failures += len(dependency_errors)
        _fail("Dependencies")
        for err in dependency_errors:
            print(f"  - {err}")
        print("  - Config and paths check skipped due to dependency failures.")
    else:
        _ok("Dependencies")
        config_errors = _check_config_and_paths()
        if config_errors:
            failures += len(config_errors)
            _fail("Config and paths")
            for err in config_errors:
                print(f"  - {err}")
        else:
            _ok("Config and paths")

    if failures:
        print(f"\nHealth check failed with {failures} issue(s).")
        return 1
    print("\nHealth check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
