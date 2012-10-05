
import sys
import os
import signal
import subprocess
import time

from zmq.eventloop.ioloop import PeriodicCallback

from cc.crypto import CryptoContext
from cc.handler import CCHandler
from cc.message import CCMessage
from cc.reqs import ErrorMessage, JobConfigReplyMessage
from cc.job import make_job_defaults

import skytools

__all__ = ['JobMgr']

CC_HANDLER = 'JobMgr'

#
# JobMgr
#

TIMER_TICK = 2

class JobState:
    log = skytools.getLogger('h:JobState')

    def __init__(self, jname, jcf, cc_url, ioloop, xtx):
        self.jname = jname
        self.jcf = jcf
        self.proc = None
        self.cc_url = cc_url
        self.timer = None
        self.ioloop = ioloop
        self.pidfile = jcf.getfile('pidfile')
        self.start_count = 0
        self.start_time = None
        self.dead_since = None

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
            try:
                data = self.proc.stdout.read()
            except IOError, e:
                if e.errno != 35: raise
                self.log.info ('checking on %s (%i) - %s', self.jname, self.proc.pid, e)
                return
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

    def start (self, args_extra = []):
        # unsure about the best way to specify target
        mod = self.jcf.get('module', '')
        script = self.jcf.get('script', '')
        cls = self.jcf.get('class', '')
        args = [self.jcf.filename, self.jname]
        args.extend (args_extra)
        if mod:
            cmd = ['python', '-m', mod] + args
        elif script:
            cmd = [script] + args
        else:
            raise skytools.UsageError('JobState.start: dunno how to launch class')

        self.log.info('Launching %s: %s', self.jname, " ".join(cmd))
        if sys.platform == 'win32':
            p = subprocess.Popen(cmd, close_fds = True)
            self.proc = None
        else:
            cmd.append('-d')
            p = subprocess.Popen(cmd, close_fds = True,
                                stdin = open(os.devnull, 'rb'),
                                stdout = subprocess.PIPE,
                                stderr = subprocess.STDOUT)
            skytools.set_nonblocking(p.stdout, True)
            self.proc = p

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
            self.log.info('Signalling %s', self.jname)
            skytools.signal_pidfile(self.pidfile, signal.SIGINT)
        except:
            self.log.exception('signal_pidfile failed: %s', self.pidfile)


class JobMgr(CCHandler):
    """Provide config to local daemons / tasks."""

    log = skytools.getLogger('h:JobMgr')

    CC_ROLES = ['local']

    def __init__(self, hname, hcf, ccscript):
        super(JobMgr, self).__init__(hname, hcf, ccscript)

        self.cc_config = ccscript.args[0]

        self.local_url = ccscript.local_url
        self.cc_job_name = ccscript.job_name

        self.job_args_extra = []
        if ccscript.options.quiet:
            self.job_args_extra.append("-q")
        if ccscript.options.verbose:
            self.job_args_extra.extend(["-v"] * ccscript.options.verbose)

        self.jobs = {}
        for dname in self.cf.getlist('daemons'):
            defs = make_job_defaults(ccscript.cf, dname)
            self.add_job(dname, defs)

        self.xtx = CryptoContext(None)

    def add_job(self, jname, defs):
        jcf = skytools.Config(jname, self.cf.filename, user_defs = defs)
        j = JobState(jname, jcf, self.local_url, self.ioloop, self.xtx)
        self.jobs[jname] = j
        j.start(self.job_args_extra)

    def handle_msg(self, cmsg):
        """ Got message from client, answer it. """

        self.log.warning('JobMgr req: %s', cmsg)
        return

    def stop(self):
        super(JobMgr, self).stop()
        self.log.info('Stopping CC daemons')
        for j in self.jobs.values():
            self.log.debug("stopping %s", j.jname)
            j.stop()
