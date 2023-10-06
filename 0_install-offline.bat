@echo off

PUSHD %~DP0

for %%i in (%~dp0..) do set ParrentPath=%%~fi

type "%ParrentPath%\exem_analysis_module.txt"

SET WORK_DIR=%~DP0
SET PYTHON_DIR=%ParrentPath%\python-3.8.10-embed-amd64\

%PYTHON_DIR%python.exe -m virtualenv .venv
call .venv\Scripts\activate

pip install --no-index --find-links="package/setup/" -r package/requirements.txt
deactivate

pause
