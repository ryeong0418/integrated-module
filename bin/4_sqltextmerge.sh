#! /bin/bash
source ../.venv/bin/activate
echo 'python virtual environment activating..'
sleep 1
nohup python ../smart_analyzer.py --proc m --s_date $1 --interval $2 &
deactivate
