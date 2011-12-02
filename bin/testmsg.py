#! /usr/bin/env python

""" logs to cc
"""

import sys
import zmq
import time
import uuid
import json

url = 'tcp://127.0.0.1:10000'
quiet = 0

if len(sys.argv) < 2:
    print 'usage: testmsg log|task|info|db|job [cc-url]'
    sys.exit(0)
if len(sys.argv) > 2:
    url = sys.argv[2]
if sys.argv[-1] == '-q':
    quiet = 1

zctx = zmq.Context()
sock = zctx.socket(zmq.XREQ)
sock.connect(url)

now = time.time()
typ = sys.argv[1]
need_answer = True
if typ == 'info':
    msg = {'req': 'pub.infofile', 'time': now, 'mtime': 1314187603, 'hostname': 'me', 'filename': 'info.1', 'data': 'qwerty'.encode('base64'), 'comp': '', 'mode': 'b'}
    need_answer = False
elif typ == 'taska':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'time': now, 'task_host': 'hostname', 'task_handler': 'cc.task.sample_async', 'task_id': 55}
elif typ == 'task':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'time': now, 'task_host': 'hostname', 'task_handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'run'}
elif typ == 'task1':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'time': now, 'task_host': 'hostname', 'task_handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'crash-launch'}
elif typ == 'task2':
    msg = {'req': 'task.send.%s' % uuid.uuid1(), 'time': now, 'task_host': 'hostname', 'task_handler': 'cc.task.sample', 'task_id': 55, 'cmd': 'crash-run'}
elif typ == 'log':
    msg = {'req': 'log.info', 'time': now, 'log_time': now, 'hostname': 'host', 'job_name': 'job', 'log_level': 'INFO', 'log_msg': 'Foo'}
    need_answer = False
elif typ == 'db':
    msg = {'req': 'confdb', 'time': now, 'host': 'hostname', 'function': 'public.test_json'}
elif typ == 'job':
    msg = {'req': 'job.config', 'time': now}
    msg = {'req': 'job.config', 'time': now, 'job_name': 'qwerty'}
    msg = {'req': 'job.config', 'time': now, 'job_name': 'd:taskrunner'}
else:
    print 'unknown type'
    sys.exit(1)

mjs = json.dumps(msg)
zmsg = ['', msg['req'], mjs, '']

if not quiet:
    print 'request:', repr(zmsg)
sock.send_multipart(zmsg)
if typ == 'task':
    sock.send_multipart(zmsg) # should ignore

if need_answer:
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

sys.exit(0)

