#! /bin/bash

python -m venv ../.venv
sleep 0.5
source ../.venv/bin/activate
sleep 0.5
pip install -r ../package/requirements.txt
deactivate