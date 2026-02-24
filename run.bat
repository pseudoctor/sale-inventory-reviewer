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

python -c "import pandas, openpyxl, yaml, xlrd" >nul 2>&1
if errorlevel 1 (
  set "REQ_FILE=requirements.txt"
  if exist "requirements.lock" set "REQ_FILE=requirements.lock"
  echo Installing missing dependencies from !REQ_FILE! ...
  python -m pip install -r !REQ_FILE!
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
