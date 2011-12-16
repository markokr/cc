"""Infofile tests"""

import os, os.path
import socket
import time
import unittest

from cc.test import CCTestCase, VMAP, TMPDIR, waitfile

class TestInfofile(CCTestCase):
    """Test infofile.

    info-recv.ini::

        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = remote
        cc-socket = PORT1

        [routes]
        pub.infofile = h:infowriter

        [h:infowriter]
        handler = cc.handler.infowriter
        dstdir = TMP/dst-infofile
        host-subdirs = yes
        bakext = --prev

    info-send.ini::

        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = local
        cc-socket = PORT2

        [routes]
        pub = h:proxy
        job = h:jobmgr

        [h:proxy]
        handler = cc.handler.proxy
        remote-cc = PORT1

        [h:jobmgr]
        handler = cc.handler.jobmgr
        pidfiledir = TMP
        daemons = d:infosender, d:infoscript

        [d:infosender]
        module = cc.daemon.infosender
        infodir = TMP/src-infofile
        infomask = info.*
        compression = gzip
        compression-level = 1
        use-blob = 1

        [d:infoscript]
        module = cc.daemon.infoscript
        info-name = dmesg
        info-script = ps axuw
        info-period = 5
        compression = gzip
        use-blob = 1
    """

    def setUp(self):
        dir1 = os.path.join(TMPDIR, 'src-infofile')
        dir2 = os.path.join(TMPDIR, 'dst-infofile')
        os.system('rm -rf ' + dir1)
        os.system('rm -rf ' + dir2)
        os.system('mkdir -p %s %s' % (dir1, dir2)) 

        CCTestCase.setUp(self)

    def runTest(self):
        hostname = socket.gethostname()

        # send info msg
        e = os.system('./bin/testmsg.py info ' + VMAP['PORT2'] + ' -q')
        self.assertEqual(e, 0)
        e = os.system('grep -q Except %s/*.log' % TMPDIR);
        self.assertNotEqual(e, 0)

        fn = os.path.join(TMPDIR, 'dst-infofile', hostname, 'dmesg')
        waitfile(fn)

        fn = os.path.join(TMPDIR, 'dst-infofile', 'me', 'info.1')
        waitfile(fn)

if __name__ == '__main__':
    unittest.main()

