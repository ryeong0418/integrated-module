@echo on
call .venv\Scripts\activate
start /b python smart_analyzer.py --proc m --s_date %1 --interval %2
pause
