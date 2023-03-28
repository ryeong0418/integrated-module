@echo on
python -m venv .venv
call .venv\Scripts\activate
pip install --no-index --find-links="package/setup/" -r package/requirements.txt
deactivate
pause