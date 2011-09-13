#! /usr/bin/env python

""" logs to cc
"""

import sys
import zmq
import time
import uuid
import json

zctx = zmq.Context()
sock = zctx.socket(zmq.XREQ)
sock.connect('tcp://127.0.0.1:10000')

if len(sys.argv) < 2:
    print 'usage: testmsg log|task|info|db|job'
    sys.exit(0)

typ = sys.argv[1]
if typ == 'info':
    msg = {'req': 'pub.infofile', 'mtime': 1314187603, 'hostname': 'me', 'filename': 'info.1', 'data': 'qwerty'.encode('base64'), 'comp': ''}
elif typ == 'taska':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'host': 'hostname', 'handler': 'cc.task.sample_async', 'task_id': 55}
elif typ == 'task':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'host': 'hostname', 'handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'run'}
elif typ == 'task1':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'host': 'hostname', 'handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'crash-launch'}
elif typ == 'task2':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'host': 'hostname', 'handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'crash-run'}
elif typ == 'log':
    msg = {'req': 'pub.log', 'host': 'hostname', 'msg': 'Foo'}
elif typ == 'db':
    msg = {'req': 'confdb', 'host': 'hostname', 'function': 'public.test_json'}
elif typ == 'job':
    msg = {'req': 'job.config'}
    msg = {'req': 'job.config', 'job_name': 'qwerty'}
    msg = {'req': 'job.config', 'job_name': 'd:taskrunner'}
else:
    print 'unknown type'
    sys.exit(0)

mjs = json.dumps(msg)
zmsg = ['', msg['req'], mjs, '']

print 'request:', repr(zmsg)
sock.send_multipart(zmsg)
if typ == 'task':
    sock.send_multipart(zmsg) # should ignore

res = sock.recv_multipart()
print 'response:', repr(res)

if typ[:4] == 'task':
    while True:
        res = sock.recv_multipart()
        print 'response:', repr(res)
        data = json.loads (res[2])
        if 'feedback' in data:
            fb = data['feedback']
            if 'out' in fb:
                print fb['out'].decode('base64')
        if data['status'] in ('finished', 'failed'):
            break
