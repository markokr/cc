#! /bin/sh

set -e

PATH=`pwd`/bin:/opt/apps/mongrel/bin:$PATH
export PATH

PYTHONPATH=`pwd`:$PYTHONPATH
export PYTHONPATH

mkdir -p var/log var/run var/infofiles

echo "starting local cc"
ccserver.py -d conf/cclocal.ini

echo "starting central cc"
ccserver.py -d conf/ccserver.ini

# echo "starting local task executor"
# cctaskrunner.py -d conf/cctaskrunner.ini

# echo 'sending task'
# testmsg.py task

while [ True ]; do
    sleep 1
    echo "checking if infofile arrived"
    ls var/infofiles
done

# sh kill.sh
