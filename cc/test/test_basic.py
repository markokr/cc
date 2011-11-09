"""Basic tests"""

import time
import os, os.path
import unittest

from cc.test import CCTestCase, VMAP, TMPDIR

class TestLogging(CCTestCase):
    """Test logging.

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

    log-send.ini::

        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = local
        cc-socket = PORT2

        [routes]
        log = h:proxy

        [h:proxy]
        handler = cc.handler.proxy
        remote-cc = PORT1
    """

    def runTest(self):
        # send log msg, check if reached dst
        e = os.system('./bin/testmsg.py log ' + VMAP['PORT2'] + ' -q')
        self.assertEqual(e, 0)
        time.sleep(0.5)
        e = os.system('grep -q Foo %s/log-recv.log' % TMPDIR);
        self.assertEqual(e, 0)

        # send info msg - unhandled, should not crash
        e = os.system('./bin/testmsg.py info ' + VMAP['PORT2'] + ' -q')
        self.assertEqual(e, 0)
        e = os.system('grep -q "no route" %s/log-send.log' % TMPDIR);
        self.assertEqual(e, 0)
        e = os.system('grep -q Except %s/*.log' % TMPDIR);
        self.assertNotEqual(e, 0)


if __name__ == '__main__':
    unittest.main()

