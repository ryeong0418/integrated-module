@echo off

FOR /F %%i IN (./tmp/pid.tmp) DO set pid=%%i

tasklist /FI "IMAGENAME eq python.exe" | find "%pid%" > nul

IF NOT ERRORLEVEL 1 (
    echo "Aanalyzer Module Batch Already Working.."
    echo "Restart Analyzer Module Batch!!"
    taskkill /f /PID %pid%
    timeout 1 > nul
) else (
    echo "Aanalyzer Module Batch Not Working.."
    echo "Start Analyzer Module Batch!!"
)

call .venv/Scripts/activate
start /b  python smart_analyzer.py --proc b
