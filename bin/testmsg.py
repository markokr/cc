#! /usr/bin/env python

""" logs to cc
"""

import sys
import zmq
import time
import json

zctx = zmq.Context()
sock = zctx.socket(zmq.XREQ)
sock.connect('tcp://127.0.0.1:10000')

if len(sys.argv) < 2:
    print 'usage: testmsg log|task|info'
    sys.exit(0)

typ = sys.argv[1]
if typ == 'info':
    msg = {'req': 'pub.infofile', 'msg': 'Blah'}
elif typ == 'task':
    msg = {'req': 'req.task.send', 'host': 'hostname'}
elif typ == 'log':
    msg = {'req': 'pub.log', 'host': 'hostname', 'msg': 'Foo'}
elif typ == 'db':
    msg = {'req': 'confdb', 'host': 'hostname', 'function': 'public.test_json'}
else:
    print 'unknown type'
    sys.exit(0)

mjs = json.dumps(msg)
zmsg = ['', msg['req'], mjs, '']

print 'request:', repr(zmsg)
sock.send_multipart(zmsg)

res = sock.recv_multipart()
print 'response:', repr(res)

