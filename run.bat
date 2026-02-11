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
  %PY_CMD% -m pip install --upgrade pip
  pip install -r requirements.txt
) else (
  call venv\Scripts\activate.bat
)

%PY_CMD% -c "import pandas, openpyxl, yaml, xlrd" >nul 2>&1
if errorlevel 1 (
  echo Installing missing dependencies from requirements.txt ...
  pip install -r requirements.txt
)

%PY_CMD% scripts\generate_inventory_risk_report.py

echo Generation complete. Reports written under: reports\
echo Batch mode summary: reports\batch_run_summary.xlsx
if /I not "%CI%"=="true" (
  if /I not "%NO_PAUSE%"=="1" pause
)
