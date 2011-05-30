#! /usr/bin/env python

""" logs to cc
"""

import sys
import zmq
import time
import json

zctx = zmq.Context()
sock = zctx.socket(zmq.REQ)
sock.connect('tcp://127.0.0.1:9888')

while 1:
    js = {'req': 'pub.infofile', 'msg': 'Blah'}
    mjs = json.dumps(js)
    print mjs
    sock.send_multipart([js['req'], mjs])
    res = sock.recv_multipart()
    time.sleep(5)
