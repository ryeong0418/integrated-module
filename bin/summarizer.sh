#! /bin/bash
source ../.venv/bin/activate
cd ../&& python smart_analyzer.py --proc s --s_date $1 --interval $2