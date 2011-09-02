#! /usr/bin/env python

"""Keeps track of ZMQ router for task runners.

- Sends 'task.register' message priodically.
- Executes received tasks.

"""

import subprocess
import sys

from cc import json
from cc.daemon import CCDaemon
from cc.message import CCMessage
from cc.reqs import TaskRegisterMessage, TaskReplyMessage
from cc.stream import CCStream

import zmq, zmq.eventloop
from zmq.eventloop.ioloop import PeriodicCallback

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
        self.maint_period = self.cf.getint ('maint-period', 5 * 60)

        self.periodic_reg()
        self.timer = PeriodicCallback(self.periodic_reg, self.maint_period*1000, self.ioloop)
        self.timer.start()

    def handle_cc_recv(self, zmsg):
        """Got task, do something with it"""
        try:
            cmsg = CCMessage(zmsg)
            self.launch_task(cmsg)
        except:
            self.log.exception('TaskRunner.handle_cc_recv crashed, dropping msg')

    def launch_task(self, cmsg):
        """Parse and execute task."""

        self.log.info("TaskRunner.launch_task: %s", cmsg)

        msg = cmsg.get_payload(self.xtx)

        jname = 'task_%i' % msg.task_id
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

        self.log.info('Launched task: %s', ' '.join(cmd))
        out = p.communicate(js)[0]
        self.log.info('Task returned: rc=%d, out=%r', p.returncode, out)

        req = cmsg.get_dest()
        uid = req.split('.')[2]
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % uid,
                handler = msg['handler'],
                task_id = msg['task_id'],
                status = 'launched')
        self.ccpublish (rep)

    def work(self):
        """Default work loop simply runs ioloop."""
        self.log.info('Starting IOLoop')
        self.ioloop.start()
        return 1

    def periodic_reg(self):
        """Register taskrunner in central router."""
        msg = TaskRegisterMessage (req = 'task.register', host = self.local_id)
        self.log.info ('TaskRunner.periodic_reg: %s', repr(msg))
        self.ccpublish (msg)

if __name__ == '__main__':
    s = TaskRunner('task_runner', sys.argv[1:])
    s.start()
