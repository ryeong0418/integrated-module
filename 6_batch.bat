@echo on

call .venv/Scripts/activate
start /b  python smart_analyzer.py --proc b
