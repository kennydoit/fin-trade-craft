@echo off
REM Activate the Python virtual environment
REM Usage: Double-click this file or run "activate_env.bat" from command line

echo Activating virtual environment...
call .\.venv\Scripts\activate

echo.
echo Virtual environment activated!
echo Python location: %VIRTUAL_ENV%
echo.
echo You can now run Python scripts or install packages.
echo Type 'deactivate' to exit the virtual environment.
echo.

REM Keep the command prompt open after activation
cmd /k
