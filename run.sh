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
  pip install --upgrade pip
  pip install -r requirements.txt
else
  source venv/bin/activate
fi

# Ensure required runtime deps exist even for old pre-created venv.
if ! python3 -c "import pandas, openpyxl, yaml, xlrd" >/dev/null 2>&1; then
  echo "Installing missing dependencies from requirements.txt ..."
  pip install -r requirements.txt
fi

python3 scripts/generate_inventory_risk_report.py

echo "Generation complete. Reports written under: reports/"
echo "Batch mode summary: reports/batch_run_summary.xlsx"
