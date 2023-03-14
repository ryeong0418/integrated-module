@echo on
call .venv\Scripts\activate
python smart_analyzer.py --proc m --s_date %1 --interval %2
pause