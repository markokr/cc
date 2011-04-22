#! /bin/sh

# requires -9 for some webscale reasons...
killall -9 mongrel2

pkill -f hots

rm -rf ./var/log ./var/run
mkdir -p var/log var/run

