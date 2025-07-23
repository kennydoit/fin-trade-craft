@echo off
echo Running %1 with fin-trade-craft virtual environment...
echo Working directory: %~dp1
echo ============================================================

cd /d "%~dp1"
"C:\Users\Kenrm\repositories\fin-trade-craft\.venv\Scripts\python.exe" "%1"

echo.
echo ============================================================
echo Execution completed.
echo.
pause
