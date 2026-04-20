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

for /f %%i in ('python -c "from pathlib import Path; import sys; sys.path.insert(0, str(Path('.').resolve())); from scripts.core import config as core_config; config_path = Path('config.yaml'); needs_xls = False
if config_path.exists():
    try:
        config = core_config.load_config(config_path)
        def is_xls_file(name):
            return isinstance(name, str) and name.strip().lower().endswith('.xls')
        if is_xls_file(config.get('inventory_file')) or is_xls_file(config.get('carton_factor_file')):
            needs_xls = True
        if any(is_xls_file(name) for name in config.get('sales_files', [])):
            needs_xls = True
        if str(config.get('run_mode', 'single')).lower() == 'batch':
            for system in config.get('batch', {}).get('systems', []):
                if not bool(system.get('enabled', True)):
                    continue
                if is_xls_file(system.get('inventory_file')) or is_xls_file(system.get('carton_factor_file')) or any(is_xls_file(name) for name in system.get('sales_files', [])):
                    needs_xls = True
                    break
    except Exception:
        pass
print('yes' if needs_xls else 'no')"') do set "HAS_XLS_SUPPORT=%%i"

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
