@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
  echo Error: Python is not installed or not in PATH
  pause
  exit /b 1
)

if not exist "venv\" (
  python -m venv venv
  call venv\Scripts\activate.bat
  python -m pip install --upgrade pip
  pip install -r requirements.txt
) else (
  call venv\Scripts\activate.bat
)

python scripts\generate_inventory_risk_report.py

echo Report written to: reports\inventory_risk_report.xlsx
pause
