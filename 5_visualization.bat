@echo off

PUSHD %~DP0

for %%i in (%~dp0..) do set ParrentPath=%%~fi

type "%ParrentPath%\exem_analysis_module.txt"

SET result="unknown error"

call .venv\Scripts\activate
python smart_analyzer.py --proc v > Output

SET /p result=<Output

echo.
echo %result%
echo.

DEL Output

pause