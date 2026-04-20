#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v python3 &> /dev/null; then
  echo "Error: python3 is not installed"
  exit 1
fi

if [ ! -d "venv" ]; then
  python3 -m venv venv
  source venv/bin/activate
  python3 -m pip install --upgrade pip
  REQ_FILE="requirements.txt"
  if [ -f "requirements.lock" ]; then
    REQ_FILE="requirements.lock"
  fi
  python3 -m pip install -r "$REQ_FILE"
else
  source venv/bin/activate
fi

HAS_XLS_SUPPORT="$(python3 - <<'PY'
from pathlib import Path
import sys

sys.path.insert(0, str(Path('.').resolve()))
try:
    from scripts.core import config as core_config
except Exception:
    print("unknown")
    raise SystemExit(0)

config_path = Path("config.yaml")
if not config_path.exists():
    print("unknown")
    raise SystemExit(0)

try:
    config = core_config.load_config(config_path)
except Exception:
    print("unknown")
    raise SystemExit(0)

def is_xls_file(name):
    return isinstance(name, str) and name.strip().lower().endswith(".xls")

needs_xls = False
if is_xls_file(config.get("inventory_file")) or is_xls_file(config.get("carton_factor_file")):
    needs_xls = True
if any(is_xls_file(name) for name in config.get("sales_files", [])):
    needs_xls = True
if str(config.get("run_mode", "single")).lower() == "batch":
    for system in config.get("batch", {}).get("systems", []):
        if not bool(system.get("enabled", True)):
            continue
        if is_xls_file(system.get("inventory_file")) or is_xls_file(system.get("carton_factor_file")):
            needs_xls = True
            break
        if any(is_xls_file(name) for name in system.get("sales_files", [])):
            needs_xls = True
            break

print("yes" if needs_xls else "no")
PY
)"

RUNTIME_IMPORTS="import pandas, openpyxl, yaml"
if [ "$HAS_XLS_SUPPORT" = "yes" ]; then
  RUNTIME_IMPORTS="import pandas, openpyxl, yaml, xlrd"
fi

# Ensure core runtime deps exist even for old pre-created venv.
if ! python3 -c "import pandas, openpyxl, yaml" >/dev/null 2>&1; then
  REQ_FILE="requirements.txt"
  if [ -f "requirements.lock" ]; then
    REQ_FILE="requirements.lock"
  fi
  echo "Installing missing dependencies from ${REQ_FILE} ..."
  python3 -m pip install -r "$REQ_FILE"
fi

if [ "$HAS_XLS_SUPPORT" = "yes" ] && ! python3 -c "import xlrd" >/dev/null 2>&1; then
  REQ_FILE="requirements.txt"
  if [ -f "requirements.lock" ]; then
    REQ_FILE="requirements.lock"
  fi
  echo "Installing xls support from ${REQ_FILE} ..."
  python3 -m pip install -r "$REQ_FILE"
fi

python3 scripts/health_check.py
python3 scripts/generate_inventory_risk_report.py

echo "Generation complete. Reports written under: reports/"
echo "Batch mode summary: reports/batch_run_summary.xlsx"
