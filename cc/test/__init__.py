"""CC regression tests."""

import sys
import time
import os, os.path
import unittest
import subprocess
import skytools
import signal
import glob
import errno

import cc.util

TMPDIR = "./regtest"

# these vars are replaced into config
VMAP = {
    'TMP': TMPDIR,
    'PORT1': 'tcp://127.0.0.1:3001',
    'PORT2': 'tcp://127.0.0.1:3002',
    'PORT3': 'tcp://127.0.0.1:3003',
}

def waitfile(fn):
    """Wait some time until file appears."""
    end = time.time() + 5
    while 1:
        if os.path.isfile(fn):
            return True
        now = time.time()
        if now > end:
            return False
        time.sleep(0.2)

def parsekonf(docstr):
    """Load config items from docstring.
    
    Returns dict of fn->body.
    """
    parts = docstr.split('::')
    conf = {}
    for i in range(1, len(parts)):
        p0 = parts[i-1]
        p1 = parts[i]
        pos = p0.rfind('\n')
        if pos < 0:
            pos = 0
        fn = p0[pos : ].strip()

        if i < len(parts) - 1:
            pos = p1.rfind('\n')
            p1 = p1[ : pos + 1]

        cf = skytools.dedent(p1)
        for k, v in VMAP.items():
            cf = cf.replace(k, v)
        fn = os.path.join(TMPDIR, fn)
        conf[fn] = cf

    return conf

class CCTestCase(unittest.TestCase):
    """docstr contains configs:

    log-recv.ini::

        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = remote
        cc-socket = PORT1
        [routes]
        log = h:locallog
        [h:locallog]
        handler = cc.handler.locallogger

    """

    def setUp(self):

        try:
            os.mkdir(TMPDIR)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise

        # killall
        for fn in glob.glob(TMPDIR + '/*.pid'):
            skytools.signal_pidfile(fn, signal.SIGTERM)

        # init new
        konf = parsekonf(self.__doc__)
        for fn, body in konf.items():
            #print 'setUp', fn
            pf = fn.replace('.ini', '.pid')
            lf = fn.replace('.ini', '.log')

            # clean old files with same name
            skytools.signal_pidfile(pf, signal.SIGTERM)
            try:
                os.unlink(lf)
            except OSError:
                pass
            try:
                os.unlink(pf)
            except OSError:
                pass

            f = open(fn, 'w')
            f.write(body)
            f.close()

            self.runcc(fn, '-d')

        time.sleep(5)
        for fn in konf.keys():
            pf = fn.replace('.ini', '.pid')
            lf = fn.replace('.ini', '.log')
            if skytools.signal_pidfile(pf, 0):
                continue
            if os.path.isfile(lf):
                #os.system("tail " + lf)
                pass

            self.fail('process did not start: %s' % fn)

    def tearDown(self):
        konf = parsekonf(self.__doc__)
        for fn, body in konf.items():
            #print 'tearDown', fn
            pf = fn.replace('.ini', '.pid')
            skytools.signal_pidfile(pf, signal.SIGHUP)

        time.sleep(0.5)
        for fn, body in konf.items():
            pf = fn.replace('.ini', '.pid')
            skytools.signal_pidfile(pf, signal.SIGTERM)

        # recheck
        time.sleep(0.5)
        for fn in glob.glob(TMPDIR + '/*.pid'):
            got = skytools.signal_pidfile(fn, signal.SIGTERM)
            if got:
                #print "Found leftover process:", fn
                bfn = os.path.basename(fn)
                sys.stdout.write("[%s] " % bfn)
                sys.stdout.flush()

        e = os.system("grep Except %s/*.log" % TMPDIR)
        if e == 0:
            self.fail('errors found')


    def runcc(self, conf, args):
        cmdline = "%s -m cc.server %s %s" % (sys.executable, args, conf)
        cmdline = cmdline.split(' ')
        return self.runcmd(cmdline)

    def runcmd(self, cmdline, checkerr = True):
        p = subprocess.Popen(cmdline, stderr = subprocess.STDOUT, stdin = subprocess.PIPE, stdout = subprocess.PIPE)
        p.stdin.close()
        skytools.set_nonblocking(p.stdout, 1)

        # loop some time
        end = time.time() + 5
        out = []
        while 1:
            if p.poll() is not None:
                break
            self.pread(p, out)
            now = time.time()
            if now > end:
                p.terminate()
                p.wait()
                self.pread(p, out)
                res = ''.join(out)
                self.fail("cmdline takes too much time: %s [%s]" % (repr(cmdline), res))
            time.sleep(0.2)
        self.pread(p, out)
        res = ''.join(out)
        if p.returncode != 0 and checkerr:
            self.fail("cmdline failed: %s (%s)" % (cmdline, res))
        return res

    def pread(self, p, outarr):
        try:
            blk = p.stdout.read(1024)
            if blk:
                outarr.append(blk)
        except IOError:
            pass

