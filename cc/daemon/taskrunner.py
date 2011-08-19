#! /usr/bin/env python

"""Keeps track of ZMQ router for task runners.

- Sends 'task.register' message priodically.
- Executes received tasks.

"""

import sys
import os, os.path
import subprocess

from cc import json
from cc.daemon import CCDaemon
from cc.message import CCMessage

import zmq, zmq.eventloop
from zmq.eventloop.ioloop import PeriodicCallback

from cc import json
from cc.stream import CCStream

import skytools

class TaskRunner(CCDaemon):
    """Register as handler for host.

    Receive and process tasks.
    """

    def startup(self):
        super(TaskRunner, self).startup()

        self.ioloop = zmq.eventloop.IOLoop.instance()
        self.connect_cc()
        self.ccs = CCStream(self.cc, self.ioloop)
        self.ccs.on_recv(self.handle_cc_recv)

        self.local_id = self.cf.get('local-id', self.hostname)

        self.periodic_reg()
        self.maint_period = 5*60
        self.timer = PeriodicCallback(self.periodic_reg, self.maint_period*1000, self.ioloop)
        self.timer.start()

    def handle_cc_recv(self, zmsg):
        """Got task, do something with it"""
        try:
            cmsg = CCMessage(zmsg)
            self.launch_task(cmsg)
        except:
            self.log.exception('task launcher crashed')

    def launch_task(self, cmsg):
        """Parse and execute task."""

        self.log.info("TaskRunner: %s", cmsg)

        msg = cmsg.get_payload(self.xtx)
        js = msg.dump_json()

        jname = 'task_%s' % msg.task_id
        info = {'task': msg,
                'config': {
                    'pidfile': self.pidfile + '.' + jname,
                }}
        js = json.dumps(info)

        mod = msg['handler']
        cmd = ['python', '-m', mod, '--cc', self.options.cc, '--cctask', jname, '-d']
        p = subprocess.Popen(cmd, 0,
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        out = p.communicate(js)[0]
        self.log.info('launch task: %s - ret=%d, out=%r', ' '.join(cmd), p.returncode, out)

    def work(self):
        """Default work loop simply runs ioloop."""
        self.log.info('Starting ioloop')
        self.ioloop.start()
        return 1

    def periodic_reg(self):
        """Register taskrunner in central router."""
        req = {'req': 'task.register', 'host': 'hostname'}
        zmsg = ['', req['req'], json.dumps(req)]
        self.log.info('maint: %s', repr(zmsg))
        self.cc.send_multipart(zmsg)

if __name__ == '__main__':
    s = TaskRunner('task_runner', sys.argv[1:])
    s.start()

