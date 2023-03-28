@echo on
python -m venv .venv
call .venv\Scripts\activate
pip install -r package/requirements.txt
deactivate
pause