@echo off
call .venv/Scripts/activate
start /b  python smart_analyzer.py --proc t --s_date %1 --interval %2
