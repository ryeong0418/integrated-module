@echo off

PUSHD %~dp0

echo %~dp0

for %%i in (%~dp0..) do set ParentPath=%%~fi 

set ParentPath=%ParentPath: =%

echo %ParentPath%

SET ZULU_DIR=%ParentPath%\zulu8.70.0.23-ca-jdk8.0.372-win_x64

echo %ZULU_DIR%