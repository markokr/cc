#! /bin/sh

set -e

# requires -9 for some webscale reasons...
if ps -ef | grep -v grep | grep mongrel2
then
    echo "killing mongrel2"
    killall -9 mongrel2
fi

for f in var/run/*.pid; do
  if test -f "$f"; then
    echo "killing $f"
    kill `cat $f`
  fi
done

#rm -rf ./var/log ./var/run ./var/info* ./infodir/info*
#mkdir -p var/log var/run

sleep 1

ps aux|grep '[p]ython'


