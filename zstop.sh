#! /bin/sh

#set -e

#for f in ~/pid/*.pid; do
#  if test -f "$f"; then
#    echo "stopping $f"
#    kill -s INT "`cat $f`"
#  fi
#done

echo "stopping cc.daemon's"
pkill -INT -f cc.daemon
echo "stopping cc.server's"
pkill -INT -f cc.server

#rm -rf ./var/log ./var/run ./var/info* ./infodir/info*
#mkdir -p var/log var/run

sleep 1

ps aux|grep '[p]ython'
