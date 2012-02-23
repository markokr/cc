#! /usr/bin/env python

"""Simple command-line client for task sending.
"""

import sys
import uuid
import cc.task

from cc.reqs import BaseMessage, TaskSendMessage
from cc.taskmgr import TaskManager
from skytools import DBScript
from cc.crypto import CryptoContext
from cc.stream import CCReqStream
from zmq.eventloop import IOLoop

import skytools

_usage = """
%prog [options] INI CMD [subcmd args]

Use-cases:

  List handler modules: %prog --list
  Show module details:  %prog --show
  Launch module:        %prog --send [--cc=URL] K=V [K=V ...]

Requered keys for '--send': task_handler=  task_host=
"""

DEFCONF = {
    'use_skylog': '0',
    'job_name': 'taskclient',
    #'logfmt_console_verbose': '%%(asctime)s %%(process)s %%(levelname)s [%%(name)s/%%(funcName)s] %%(message)s',
}

class TaskClient(DBScript):
    def __init__(self, service_name, args):
        super(TaskClient, self).__init__(service_name, args)

        if self.options.list:
            handlers = cc.task.get_task_handlers()
            mods = handlers.keys()
            mods.sort()
            for m in mods:
                doc = handlers[m].splitlines()[0]
                print m, '-', doc
            sys.exit(0)
        elif self.options.show:
            handlers = cc.task.get_task_handlers()
            for m in self.args:
                if m in handlers:
                    print m
                    print '-' * len(m)
                    print handlers[m]
                else:
                    print m, '-', 'not found'
            sys.exit(0)
        elif not self.options.send:
            self.log.error("Need --list, --show or --send")
            sys.exit(1)


    def init_optparse(self, p = None):
        p = super(TaskClient, self).init_optparse(p)
        p.add_option("--cc", help="CC task router location")
        p.add_option("--config", help="config file")
        p.add_option("--list", action="store_true", help="list task modules")
        p.add_option("--show", action="store_true", help="show module details")
        p.add_option("--send", action="store_true", help="send task")
        p.add_option("--sync", action="store_true", help="use sync send")
        p.set_usage(_usage.strip())
        return p

    def startup(self):
        super(TaskClient, self).startup()

        self.cc_url = self.cf.get('cc')
        self.ioloop = IOLoop.instance()
        self.xtx = CryptoContext(self.cf)
        self.ccrq = CCReqStream(self.cc_url, self.xtx, self.ioloop)
        self.taskmgr = TaskManager(self.ccrq)

    def work(self):
        self.set_single_loop(1)

        hargs = {}
        for a in self.args[1:]:
            if a.find('=') <= 0:
                raise skytools.UsageError('need key=val')
            k, v = a.split('=', 1)
            hargs[k] = v

        tid = str(uuid.uuid4())

        task = TaskSendMessage(
                req = 'task.send.' + tid,
                task_id = tid,
                **hargs)
        if self.options.sync:
            # sync approach
            ti = self.taskmgr.send_task(task)
            self.log.info('reply: %r', ti.replies)
        else:
            # async approach
            ti = self.taskmgr.send_task_async(task, self.task_cb)
            self.ioloop.start()
            self.log.info('done')

    def task_cb(self, done, rep):
        if rep:
            fb = rep.get('feedback', {})
            rc = fb.get('rc', '-1')
            self.log.info('reply: %r (%s)', rep.get('status', '?status?'), rc)
            out = fb.get('out', '')
            msg = fb.get('log_msg')
            if out:
                out = out.decode('base64')
                if out:
                    for ln in out.splitlines():
                        print '>>', ln
            elif msg:
                lev = fb.get('log_level', '???')
                print '>> %s %s' % (lev, msg)
            else:
                print '>> %r' % fb
        if done:
            self.ioloop.stop()

def main():
    script = TaskClient('taskclient', sys.argv[1:])
    script.start()

if __name__ == '__main__':
    main()

