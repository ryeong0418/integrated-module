@echo off

PUSHD %~DP0

SET PROJECT_DIR=%~DP0
SET SPHINX_DIR=%PROJECT_DIR%sphinx_docs

call .venv/Scripts/activate
cd %SPHINX_DIR%


sphinx-apidoc -f -o . ..
make html  
