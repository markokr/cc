#! /usr/bin/env python

""" logs to logserver
"""

import sys
import zmq
import time

zctx = zmq.Context()
sock = zctx.socket(zmq.PUB)
sock.connect('tcp://127.0.0.1:4030')

while 1:
    msg = 'log.info.fooza {"msg": "Blah happened"}'
    print msg
    sock.send(msg)
    time.sleep(5)
