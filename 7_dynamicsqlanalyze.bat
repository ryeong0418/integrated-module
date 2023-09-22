@echo off

PUSHD %~DP0

for %%i in (%~dp0..) do set ParentPath=%%~fi


type "%ParentPath%\exem_analysis_module.txt"

SET result="unknown error"

call .venv\Scripts\activate

if "%~1" equ "" (
    echo "다이나믹 쿼리 분석 기능 수행.."
    python smart_analyzer.py --proc d > Output
) else (
    echo "다이나믹 쿼리 파싱 기능 수행.."
    python smart_analyzer.py --proc d --s_date %1 --interval %2 > Output
)

SET /p result=<Output

echo.
echo %result%
echo.

DEL Output

pause
