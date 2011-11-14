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
import re
import logging

from cc import json
from cc.daemon import CCDaemon
from cc.message import CCMessage
from cc.reqs import TaskRegisterMessage, TaskReplyMessage
from cc.stream import CCStream

import zmq, zmq.eventloop
from zmq.eventloop.ioloop import PeriodicCallback

import skytools

_TID_INVALID = re.compile('[^-a-zA-Z0-9_]')

class TaskState (object):
    """ Tracks task state (with help of watchdog) """

    log = logging.getLogger('cc.daemon.taskrunner.TaskState')

    def __init__ (self, uid, name, info, ioloop, cc, xtx):
        self.uid = uid
        self.name = name
        self.info = info
        self.pidfile = info['config']['pidfile']
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
            self.log.debug ('Killing %s', self.name)
            skytools.signal_pidfile (self.pidfile, signal.SIGINT)
        except:
            self.log.exception ('signal_pidfile(%s) failed', self.pidfile)

    def watchdog (self):
        live = skytools.signal_pidfile (self.pidfile, 0)
        if live:
            self.log.debug ('%s is alive', self.name)
            if self.heartbeat:
                self.send_reply ('running')
        else:
            self.log.info ('%s is dead', self.name)
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
                handler = self.info['task']['task_handler'],
                task_id = self.info['task']['task_id'],
                status = status,
                feedback = feedback)
        self.ccpublish (msg)


class TaskRunner(CCDaemon):
    """Register as handler for host.

    Receive and process tasks.
    """

    log = logging.getLogger('cc.daemon.taskrunner')

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

        self.log.debug("launch_task: %s", cmsg)

        msg = cmsg.get_payload(self.xtx)
        req = cmsg.get_dest()
        tid = msg['task_id']

        if _TID_INVALID.search(tid):
            self.log.error('Invalid task id: %r', tid)
            return

        if tid in self.tasks:
            self.log.info ("Ignored task %s", tid)
            return

        jname = 'task_%s' % tid
        jpidf = self.pidfile + '.' + jname
        info = {'task': msg,
                'config': {
                    'pidfile': jpidf,
                }}
        js = json.dumps(info)

        self.task_reply(tid, 'starting')

        mod = msg['task_handler']
        cmd = ['python', '-m', mod, '--cc', self.options.cc, '--cctask', jname, '-d']
        p = subprocess.Popen(cmd, 0,
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)

        self.log.info('Launched task: %s', ' '.join(cmd))
        out = p.communicate(js)[0]
        self.log.debug('Task returned: rc=%d, out=%r', p.returncode, out)

        fb = {'rc': p.returncode, 'out': out.encode('base64')}
        if p.returncode == 0:
            st = 'launched'
        else:
            st = 'failed'
        self.task_reply(tid, st, fb)

        if p.returncode == 0:
            tstate = TaskState (tid, jname, info, self.ioloop, self.cc, self.xtx)
            tstate.heartbeat = self.task_heartbeat
            self.tasks[tid] = tstate
            tstate.start()

    def task_reply(self, tid, status, fb = {}, **kwargs):
        fb = fb or kwargs
        self.log.debug('task_reply: %r - %r', status, fb)
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % tid,
                task_id = tid,
                status = status,
                feedback = fb)
        self.log.info('msg: %r', rep)
        self.ccpublish (rep)

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
        self.log.debug ("TaskRunner.do_maint")
        now = time.time()
        for ts in self.tasks.itervalues():
            if now - ts.dead_since > self.grace_period:
                self.log.info ('TaskRunner.do_maint: forgetting task %s', ts.uid)
                del self.tasks[ts.uid]


if __name__ == '__main__':
    s = TaskRunner('task_runner', sys.argv[1:])
    s.start()
