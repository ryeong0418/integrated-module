#! /bin/bash
source ../.venv/bin/activate
echo 'python virtual environment activating..'
sleep 1
python ../smart_analyzer.py --proc i
deactivate