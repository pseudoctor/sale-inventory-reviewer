@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

set "PY_CMD="
py -3 --version >nul 2>&1
if not errorlevel 1 set "PY_CMD=py -3"

if not defined PY_CMD (
  python --version >nul 2>&1
  if not errorlevel 1 set "PY_CMD=python"
)

if not defined PY_CMD (
  echo Error: Python 3 is not installed or not in PATH
  if /I not "%CI%"=="true" (
    if /I not "%NO_PAUSE%"=="1" pause
  )
  exit /b 1
)

if not exist "venv\" (
  %PY_CMD% -m venv venv
  call venv\Scripts\activate.bat
  python -m pip install --upgrade pip
  set "REQ_FILE=requirements.txt"
  if exist "requirements.lock" set "REQ_FILE=requirements.lock"
  python -m pip install -r !REQ_FILE!
) else (
  call venv\Scripts\activate.bat
)

set "HAS_XLS_SUPPORT=no"
for /f %%i in ('python -c "from pathlib import Path; import sys; sys.path.insert(0, str(Path('.').resolve())); config_path = Path('config.yaml'); is_xls = lambda name: isinstance(name, str) and name.strip().lower().endswith('.xls'); core_config = __import__('scripts.core.config', fromlist=['config']); cfg = core_config.load_config(config_path) if config_path.exists() else None; needs_xls = bool(cfg) and (is_xls(cfg.get('inventory_file')) or is_xls(cfg.get('carton_factor_file')) or any(is_xls(name) for name in cfg.get('sales_files', [])) or (str(cfg.get('run_mode', 'single')).lower() == 'batch' and any(bool(system.get('enabled', True)) and (is_xls(system.get('inventory_file')) or is_xls(system.get('carton_factor_file')) or any(is_xls(name) for name in system.get('sales_files', []))) for system in cfg.get('batch', {}).get('systems', [])))); print('yes' if needs_xls else 'no')" 2^>nul') do set "HAS_XLS_SUPPORT=%%i"

python -c "import pandas, openpyxl, yaml" >nul 2>&1
if errorlevel 1 (
  set "REQ_FILE=requirements.txt"
  if exist "requirements.lock" set "REQ_FILE=requirements.lock"
  echo Installing missing dependencies from !REQ_FILE! ...
  python -m pip install -r !REQ_FILE!
)

if /I "%HAS_XLS_SUPPORT%"=="yes" (
  python -c "import xlrd" >nul 2>&1
  if errorlevel 1 (
    set "REQ_FILE=requirements.txt"
    if exist "requirements.lock" set "REQ_FILE=requirements.lock"
    echo Installing xls support from !REQ_FILE! ...
    python -m pip install -r !REQ_FILE!
  )
)

python scripts\health_check.py
if errorlevel 1 (
  echo Health check failed. Please fix issues above before generating report.
  if /I not "%CI%"=="true" (
    if /I not "%NO_PAUSE%"=="1" pause
  )
  exit /b 1
)

python scripts\generate_inventory_risk_report.py
if errorlevel 1 (
  echo Report generation failed.
  if /I not "%CI%"=="true" (
    if /I not "%NO_PAUSE%"=="1" pause
  )
  exit /b 1
)

echo Generation complete. Reports written under: reports\
echo Batch mode summary: reports\batch_run_summary.xlsx
if /I not "%CI%"=="true" (
  if /I not "%NO_PAUSE%"=="1" pause
)
