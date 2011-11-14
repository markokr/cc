"""Test tasks"""

import sys
import time
import os, os.path
import unittest

from cc.test import CCTestCase, VMAP, TMPDIR

class TestTasks(CCTestCase):
    """Test logging.

    task-host.ini::
        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = local
        cc-socket = PORT1
        [routes]
        task = h:proxy
        job = h:jobmgr
        [h:proxy]
        handler = cc.handler.proxy
        remote-cc = PORT2
        [h:jobmgr]
        handler = cc.handler.jobmgr
        pidfiledir = TMP
        daemons = d:taskrunner
        [d:taskrunner]
        module = cc.daemon.taskrunner
        local-id = hostname
        #reg-period = 300
        #maint-period = 60


    task-router.ini::
        [ccserver]
        logfile = TMP/%(job_name)s.log
        pidfile = TMP/%(job_name)s.pid
        cc-role = remote
        cc-socket = PORT2
        [routes]
        task = h:taskrouter
        [h:taskrouter]
        handler = cc.handler.taskrouter
        #route-lifetime = 3600
        #maint-period = 60
    """

    def runTest(self):
        # test info dump
        out = self.run_client('--list')
        self.assertTrue(out.find('sample') > 0)
        out = self.run_client('--show', 'cc.task.sample')
        self.assertTrue(out.find('Params') > 0)

        # launch task
        out = self.run_client('--send', 'cc.task.sample')
        #print out
        self.assertTrue(out.find('starting') > 0)
        self.assertTrue(out.find('success') > 0 or out.find('failed') > 0)

        # crashing task
        out = self.run_client('--send', 'cc.task.sample', 'cmd=crash-run')
        self.assertTrue(out.find('failed') > 0 or out.find('stopped') > 0)
        out = self.run_client('--send', 'cc.task.sample', 'cmd=crash-launch')
        self.assertTrue(out.find('failed') > 0 or out.find('stopped') > 0)
        out = self.run_client('--send', 'cc.task.nonexist')
        self.assertTrue(out.find('failed') > 0 or out.find('stopped') > 0)

    def run_client(self, *args):
        cmd = (sys.executable, '-m', 'cc.taskclient', '--send', '--cc', VMAP['PORT2'])
        return self.runcmd(cmd + args)

if __name__ == '__main__':
    unittest.main()

