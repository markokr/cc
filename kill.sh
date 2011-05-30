#! /bin/sh

# requires -9 for some webscale reasons...
killall -9 mongrel2

pkill -f hots
pkill -f ccserver
pkill -f cctaskrunner
pkill -f infosender

rm -rf ./var/log ./var/run ./var/info*
mkdir -p var/log var/run

