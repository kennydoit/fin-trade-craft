# PowerShell script to run Python files with the virtual environment
param(
    [string]$PythonFile
)

# Change to the directory containing the Python file
$fileDir = Split-Path -Parent $PythonFile
Set-Location $fileDir

# Activate virtual environment and run the Python file
Write-Host "Running $PythonFile with fin-trade-craft virtual environment..."
Write-Host "Working directory: $fileDir"
Write-Host "=" * 60

try {
    # Run the Python file with the virtual environment
    & "C:\Users\Kenrm\repositories\fin-trade-craft\.venv\Scripts\python.exe" $PythonFile
    
    Write-Host ""
    Write-Host "=" * 60
    Write-Host "Execution completed."
} catch {
    Write-Host "Error running Python file: $_" -ForegroundColor Red
}

# Keep the window open
Write-Host ""
Write-Host "Press any key to close..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
