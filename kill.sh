#! /bin/sh

set -e

# requires -9 for some webscale reasons...

if ps -ef | grep -v grep | grep mongrel2
then
    echo "killing mongrel2"
    killall -9 mongrel2
fi

if [ -f var/run/hots.pid ]
then
    echo "killing hots"
    kill -9 `cat var/run/hots.pid`
fi

if [ -f var/run/cclocal.pid ]
then
    echo "killing cclocal"
    kill -9 `cat var/run/cclocal.pid`
fi

if [ -f var/run/ccserver.pid ]
then
    echo "killing ccserver"
    kill -9 `cat var/run/ccserver.pid`
fi

if [ -f var/run/cctaskrunner.pid ]
then
    echo "killing cctaskrunner"
    kill -9 `cat var/run/cctaskrunner.pid`
fi

if [ -f var/run/infosender.pid ]
then
    echo "killing infosender"
    kill -9 `cat var/run/infosender.pid`
fi

rm -rf ./var/log ./var/run ./var/info* ./infodir/info*
mkdir -p var/log var/run