@echo off
setlocal
cd /d "%~dp0"

echo ========================================================
echo           SQL Importer - Starting Server               
echo ========================================================

if not exist "venv" (
    echo Creating virtual environment in .\venv...
    py -3 -m venv venv 2>nul || python -m venv venv
)

if exist "requirements.txt" (
    echo Checking dependencies...
    .\venv\Scripts\python.exe -m pip install --quiet -r requirements.txt
)

echo.
echo Server is running!
echo Open in your browser: http://127.0.0.1:5000
echo Press Ctrl+C to stop the server.
echo ========================================================
echo.

.\venv\Scripts\python.exe app.py
pause
