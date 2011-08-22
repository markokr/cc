#! /bin/sh

set -e

#PATH=`pwd`/bin:$PATH
#export PATH

#PYTHONPATH=`pwd`:$PYTHONPATH
#export PYTHONPATH

mkdir -p ~/log ~/pid
mkdir -p /tmp/infofiles

for ini in conf/*.ini; do
  echo "starting $ini"
  python -m cc.server -d $ini
done

#echo 'sending db'
#./bin/testmsg.py db
mkdir -p /tmp/infofiles

while [ True ]; do
    sleep 1
    echo "checking if infofile arrived"
    ls /tmp/infofiles/ || true
    grep -E 'ERR|WARN|CRIT|Exc' ~/log/*.log || true
done

# sh kill.sh
