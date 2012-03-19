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
        task = h:taskproxy
        job = h:jobmgr
        log = h:locallogger

        [h:locallogger]
        handler = cc.handler.locallogger

        [h:taskproxy]
        handler = cc.handler.proxy
        remote-cc = PORT2

        [h:jobmgr]
        handler = cc.handler.jobmgr
        daemons = d:taskrunner

        [d:taskrunner]
        module = cc.daemon.taskrunner
        local-id = testhost
        #reg-period = 300
        #maint-period = 60

        [cc.task.sample]
        # sudo = xx

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


    taskclient.ini::
        [taskclient]
        cc = PORT2
    """

    def runTest(self):
        # test info dump
        out = self.run_client('--list')
        self.assertTrue(out.find('sample') > 0)
        out = self.run_client('--show', 'cc.task.sample')
        self.assertTrue(out.find('Params') > 0)

        # launch task
        out = self.send_task('task_handler=cc.task.sample', 'cmd=test')
        #print 'OK?', repr(out)
        self.assertTrue(out.find('starting') > 0)
        self.assertTrue(out.find('done') > 0)

        # crashing task
        out = self.send_task('task_handler=cc.task.sample', 'cmd=crash-run')
        #print '\nCRASH_RUN', repr(out)
        self.assertTrue(out.find('failed') > 0)
        out = self.send_task('task_handler=cc.task.sample', 'cmd=crash-launch')
        #print '\nCRASH_LAUNCH', repr(out)
        self.assertTrue(out.find('finished') > 0)
        out = self.send_task('task_handler=cc.task.nonexist')
        #print '\nNONEXIST', repr(out)
        self.assertTrue(out.find('failed') > 0 or out.find('stopped') > 0)

    def send_task(self, *args):
        return self.run_client('--send', 'task_host=testhost', *args)

    def run_client(self, *args):
        cf = os.path.join(TMPDIR, 'taskclient.ini')
        cmd = (sys.executable, '-m', 'cc.taskclient', cf)
        return self.runcmd(cmd + args)

if __name__ == '__main__':
    unittest.main()
