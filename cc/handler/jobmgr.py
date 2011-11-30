
import logging
import os
import signal
import subprocess
import time

from zmq.eventloop.ioloop import PeriodicCallback

from cc.crypto import CryptoContext
from cc.handler import CCHandler
from cc.message import CCMessage
from cc.reqs import ErrorMessage, JobConfigReplyMessage
from cc.util import set_nonblocking

import skytools

__all__ = ['JobMgr']

CC_HANDLER = 'JobMgr'

#
# JobMgr
#

TIMER_TICK = 2

class JobState:
    log = logging.getLogger('h:JobState')

    def __init__(self, jname, jcf, cc_url, ioloop, pidfiledir, xtx):
        self.jname = jname
        self.jcf = jcf
        self.proc = None
        self.cc_url = cc_url
        self.timer = None
        self.ioloop = ioloop
        self.pidfile = "%s/%s.pid" % (pidfiledir, self.jname)
        self.start_count = 0
        self.start_time = None
        self.dead_since = None

        self.cfdict = {
                'job_name': self.jname,
                'pidfile': self.pidfile,
        }
        xtx.fill_config(self.cfdict)
        for o in self.jcf.options():
            self.cfdict[o] = self.jcf.get(o)

    def _watchdog_wait (self):
        # y = a + bx , apply cap
        y = self.watchdog_formula_a + self.watchdog_formula_b * (self.start_count-1)
        if (self.watchdog_formula_cap is not None
                and y > self.watchdog_formula_cap):
            y = self.watchdog_formula_cap
        return y

    def handle_timer(self):
        if self.proc:
            self.log.debug('checking on %s (%i)', self.jname, self.proc.pid)
            data = self.proc.stdout.read()
            if data:
                self.log.info('Job %s stdout: %r', self.jname, data)
            rc = self.proc.poll()
            if rc is not None:
                self.log.debug('proc exited with %s', rc)
                self.proc = None
        else:
            # daemonization successful?
            live = skytools.signal_pidfile(self.pidfile, 0)
            if live:
                self.log.debug ('%s is alive', self.jname)
                if self.start_count > 1 and time.time() > self.start_time + self.watchdog_reset:
                    self.log.debug ('resetting watchdog')
                    self.start_count = 1
            else:
                self.log.warning ('%s is dead', self.jname)
                if self.dead_since is None:
                    self.dead_since = time.time()
                if time.time() >= self.dead_since + self._watchdog_wait():
                    self.timer.stop()
                    self.timer = None
                    self.start()

    def start(self):
        # unsure about the best way to specify target
        mod = self.jcf.get('module', '')
        script = self.jcf.get('script', '')
        cls = self.jcf.get('class', '')
        args = ['-d', '--cc', self.cc_url, '--ccdaemon', self.jname]
        if mod:
            cmd = ['python', '-m', mod] + args
        elif script:
            cmd = [script] + args
        else:
            raise skytools.UsageError('JobState.start: dunno how to launch class')

        self.log.info('Launching %s: %s', self.jname, " ".join(cmd))
        self.proc = subprocess.Popen(cmd, close_fds = True,
                                stdin = open(os.devnull, 'rb'),
                                stdout = subprocess.PIPE,
                                stderr = subprocess.STDOUT)

        set_nonblocking(self.proc.stdout, True)

        self.start_count += 1
        self.start_time = time.time()
        self.dead_since = None
        self.watchdog_reset = self.jcf.getint ('watchdog-reset', 60*60)
        self.watchdog_formula_a = self.jcf.getint ('watchdog-formula-a', 0)
        self.watchdog_formula_b = self.jcf.getint ('watchdog-formula-b', 5)
        self.watchdog_formula_cap = self.jcf.getint ('watchdog-formula-cap', 0)
        if self.watchdog_formula_cap <= 0: self.watchdog_formula_cap = None

        self.timer = PeriodicCallback (self.handle_timer, TIMER_TICK * 1000, self.ioloop)
        self.timer.start()

    def stop(self):
        try:
            self.log.info('Killing %s', self.jname)
            skytools.signal_pidfile(self.pidfile, signal.SIGINT)
        except:
            self.log.exception('signal_pidfile failed: %s', self.pidfile)


class JobMgr(CCHandler):
    """Provide config to local daemons / tasks."""

    log = logging.getLogger('h:JobMgr')

    CC_ROLES = ['local']

    def __init__(self, hname, hcf, ccscript):
        super(JobMgr, self).__init__(hname, hcf, ccscript)

        self.local_url = ccscript.local_url
        self.pidfiledir = hcf.getfile('pidfiledir', '~/pid')

        self.jobs = {}
        for dname in self.cf.getlist('daemons'):
            self.add_job(dname)

        self.xtx = CryptoContext(None)

    def add_job(self, jname):
        jcf = skytools.Config(jname, self.cf.filename, ignore_defs = True)
        jstate = JobState(jname, jcf, self.local_url, self.ioloop, self.pidfiledir, self.xtx)
        self.jobs[jname] = jstate
        jstate.start()

    def handle_msg(self, cmsg):
        """ Got message from client, answer it. """

        self.log.debug('JobMgr req: %s', cmsg)
        data = cmsg.get_payload(self.xtx)
        if not data:
            return

        if data.req == 'job.config':
            if not hasattr (data, 'job_name'):
                msg = ErrorMessage(
                    req = "error.%s" % data.req,
                    msg = "Missing job_name")
            elif not data.job_name in self.jobs:
                msg = ErrorMessage(
                    req = "error.%s" % data.req,
                    job_name = data.job_name,
                    msg = "Unknown job_name")
            else:
                job = self.jobs[data.job_name]
                msg = JobConfigReplyMessage(
                    job_name = data.job_name,
                    config = job.cfdict)
        else:
            msg = ErrorMessage(
                req = "error.%s" % data.req,
                msg = 'Unsupported req')
        crep = self.xtx.create_cmsg(msg)
        crep.take_route(cmsg)
        self.cclocal.send_cmsg(crep)
        self.log.debug('JobMgr answer: %s', crep)

    def stop(self):
        self.log.info('Stopping CC daemons')
        for j in self.jobs.values():
            j.stop()
