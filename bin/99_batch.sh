pid=`cat ../tmp/pid.tmp`
proc=(`ps -ef | grep $pid | grep 'python' | grep 'proc b' | grep -v 'grep' | wc -l`)

if [ "$proc" -eq 0 ]; then
    echo "Analyzer Module Batch Not Working.."
    echo "Start Analyzer Module Batch!!"
else
    echo "Analyzer Module Batch Already Working.."
    echo "Restart Analyzer Module Batch!!"
    kill -9 $pid
fi

source ../.venv/bin/activate
echo 'python virtual environment activating..'
sleep 1
nohup python ../smart_analyzer.py --proc b &
deactivate
