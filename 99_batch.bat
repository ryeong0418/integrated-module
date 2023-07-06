@echo off

PUSHD %~dp0

for %%i in (%~dp0..) do set ParentPath=%%~fi 

set ParentPath=%ParentPath: =%

SET WORK_DIR=%~DP0
SET ZULU_DIR=%ParentPath%\zulu8.70.0.23-ca-jdk8.0.372-win_x64

SET JAVA_HOME=%ZULU_DIR%
SET CLASSPATH=%ZULU_DIR%\lib; 
SET PATH=%PATH%;%JAVA_HOME%\bin\;

call .venv/Scripts/activate
python smart_analyzer.py --proc b
