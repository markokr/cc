#! /usr/bin/env python

"""Keeps track of ZMQ router for task runners.

- Sends 'task.register' message priodically.
- Executes received tasks.
- Watches running tasks.

"""

import subprocess
import sys
import time
import signal

from cc import json
from cc.daemon import CCDaemon
from cc.message import CCMessage
from cc.reqs import TaskRegisterMessage, TaskReplyMessage
from cc.stream import CCStream

import zmq, zmq.eventloop
from zmq.eventloop.ioloop import PeriodicCallback

import skytools


class TaskState (object):
    """ Tracks task state (with help of watchdog) """

    def __init__ (self, uid, name, info, log, ioloop, cc, xtx):
        self.uid = uid
        self.name = name
        self.info = info
        self.pidfile = info['config']['pidfile']
        self.log = log
        self.ioloop = ioloop
        self.cc = cc
        self.xtx = xtx
        self.timer = None
        self.timer_tick = 1
        self.heartbeat = False
        self.start_time = None
        self.dead_since = None

    def start (self):
        self.start_time = time.time()
        self.timer = PeriodicCallback (self.watchdog, self.timer_tick * 1000, self.ioloop)
        self.timer.start()

    def stop (self):
        try:
            self.log.info ('TaskState.stop: Killing %s', self.name)
            skytools.signal_pidfile (self.pidfile, signal.SIGINT)
        except:
            self.log.exception ('TaskState.stop: signal_pidfile failed')

    def watchdog (self):
            live = skytools.signal_pidfile (self.pidfile, 0)
            if live:
                self.log.debug ('TaskState.watchdog: %s is alive', self.name)
                if self.heartbeat:
                    self.send_reply ('running')
            else:
                self.log.info ('TaskState.watchdog: %s is over', self.name)
                self.dead_since = time.time()
                self.timer.stop()
                self.timer = None
                self.send_reply ('stopped')

    def ccpublish (self, msg):
        assert isinstance (msg, TaskReplyMessage)
        cmsg = self.xtx.create_cmsg (msg)
        cmsg.send_to (self.cc)

    def send_reply (self, status, feedback = {}):
        msg = TaskReplyMessage(
                req = 'task.reply.%s' % self.uid,
                handler = self.info['task']['handler'],
                task_id = self.info['task']['task_id'],
                status = status,
                feedback = feedback)
        self.ccpublish (msg)


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
        self.reg_period = self.cf.getint ('reg-period', 5 * 60)
        self.maint_period = self.cf.getint ('maint-period', 60)
        self.grace_period = self.cf.getint ('task-grace-period', 15 * 60)
        self.task_heartbeat = self.cf.getboolean ('task-heartbeat', False)

        self.tasks = {}

        self.periodic_reg()
        self.timer_reg = PeriodicCallback (self.periodic_reg, self.reg_period * 1000, self.ioloop)
        self.timer_reg.start()
        self.timer_maint = PeriodicCallback (self.do_maint, self.maint_period * 1000, self.ioloop)
        self.timer_maint.start()

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
        req = cmsg.get_dest()
        uid = req.split('.')[2]

        if uid in self.tasks:
            self.log.info ("Ignored task %s", uid)
            return

        jname = 'task_%i' % msg.task_id
        jpidf = self.pidfile + '.' + jname
        info = {'task': msg,
                'config': {
                    'pidfile': jpidf,
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

        fb = {'rc': p.returncode, 'out': out.encode('base64')}
        if p.returncode == 0:
            st = 'launched'
        else:
            st = 'failed'
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % uid,
                handler = msg['handler'],
                task_id = msg['task_id'],
                status = st,
                feedback = fb)
        self.ccpublish (rep)

        if p.returncode == 0:
            tstate = TaskState (uid, jname, info, self.log, self.ioloop, self.cc, self.xtx)
            tstate.heartbeat = self.task_heartbeat
            self.tasks[uid] = tstate
            tstate.start()

    def work(self):
        """Default work loop simply runs ioloop."""
        self.log.info('Starting IOLoop')
        self.ioloop.start()
        return 1

    def periodic_reg(self):
        """Register taskrunner in central router."""
        msg = TaskRegisterMessage (host = self.local_id)
        self.log.info ('TaskRunner.periodic_reg: %s', repr(msg))
        self.ccpublish (msg)

    def do_maint (self):
        """ Drop old tasks (after grace period) """
        self.log.info ("TaskRunner.do_maint")
        now = time.time()
        for ts in self.tasks.itervalues():
            if now - ts.dead_since > self.grace_period:
                self.log.info ('TaskRunner.do_maint: forgetting task %s', ts.uid)
                del self.tasks[ts.uid]


if __name__ == '__main__':
    s = TaskRunner('task_runner', sys.argv[1:])
    s.start()
