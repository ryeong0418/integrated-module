@echo on
cd ./Desktop\co_dev\project
call venv\Scripts\activate
python smart_analyzer.py --proc e --s_date %1 --interval %2
pause