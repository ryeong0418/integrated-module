@echo off

PUSHD %~dp0

for %%i in (%~dp0..) do set ParentPath=%%~fi 

set ParentPath=%ParentPath: =%

type "%ParentPath%\exem_analysis_module.txt"

SET WORK_DIR=%~DP0
SET ZULU_DIR=%ParentPath%\zulu8.70.0.23-ca-jdk8.0.372-win_x64

SET PATH=%PATH%;%ZULU_DIR%\bin\;

echo %PATH%

pause