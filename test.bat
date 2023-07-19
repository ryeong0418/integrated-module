@echo off

PUSHD %~DP0

for %%i in (%~dp0..) do set ParentPath=%%~fi

type "%ParentPath%\exem_analysis_module.txt"

SET result="unknown error"

SET WORK_DIR=%~DP0
SET ZULU_DIR=%ParentPath%\zulu8.70.0.23-ca-jdk8.0.372-win_x64

setx JAVA_HOME "%ZULU_DIR%" -m
setx CLASSPATH "%ZULU_DIR%\lib;" -m
setx PATH "%PATH%;%JAVA_HOME%\bin" -m

echo %JAVA_HOME%
echo %CLASSPATH%
echo %PATH%

pause