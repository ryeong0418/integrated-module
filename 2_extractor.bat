@echo off

PUSHD %~DP0

for %%i in (%~dp0..) do set ParentPath=%%~fi

type "%ParentPath%\exem_analysis_module.txt"

SET result="unknown error"

call .venv\Scripts\activate
python smart_analyzer.py --proc e --s_date %1 --interval %2 > Output

SET /p result=<Output

echo.
echo %result%
echo.

DEL Output

pause
