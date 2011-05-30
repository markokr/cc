#! /usr/bin/env python

""" dumps log from logserver
"""

import sys
import zmq
import hots
import skytools

zctx = zmq.Context()
sock = zctx.socket(zmq.SUB)
sock.connect('tcp://127.0.0.1:4013')
sock.setsockopt(zmq.SUBSCRIBE, '')
while 1:
    msg = sock.recv()
    print msg
