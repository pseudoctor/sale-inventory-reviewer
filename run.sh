#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN=""
for candidate in python3.14 python3.13 python3.12 python3.11 python3.10 python3; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >/dev/null 2>&1; then
    PYTHON_BIN="$candidate"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "Error: Python 3.10 or newer is not installed or not in PATH"
  exit 1
fi

if [ ! -d "venv" ]; then
  "$PYTHON_BIN" -m venv venv
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

if ! python3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >/dev/null 2>&1; then
  echo "Error: the virtual environment is not using Python 3.10+"
  exit 1
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

HAS_XLS_SUPPORT="$(python3 scripts/check_xls_support_needed.py)"

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
