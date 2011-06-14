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

# echo 'sending task'
# testmsg.py task

while [ True ]; do
    sleep 1
    echo "checking if infofile arrived"
    ls /tmp/infofiles
done

# sh kill.sh
