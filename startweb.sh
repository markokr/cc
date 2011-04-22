#! /bin/sh


PATH=`pwd`/bin:/opt/apps/mongrel/bin:$PATH
export PATH

PYTHONPATH=`pwd`:$PYTHONPATH
export PYTHONPATH

mkdir -p var/log var/run

# starting mongrel

cf=conf/mongrel2.conf
db=conf/mongrel2.db
rm -f $db
m2sh load -config $cf -db $db
m2sh start -db $db -host localhost > var/log/mongrel.out 2>&1 < /dev/null &

# now mongrel should be up, launch services

hots-dbsrv.py conf/hots-dbsrv.ini -d
hots-logsrv.py conf/hots-logsrv.ini -d
hots-websrv.py conf/hots-websrv.ini -d
hots-cc.py conf/hots-cc.ini -d

echo 'starting sendlog, you can start browser now'
sendlog.py
