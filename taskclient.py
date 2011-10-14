#! /usr/bin/env python

"""Simple command-line client for task sending.
"""

import sys
import uuid
import cc.client
import cc.task

from cc.reqs import BaseMessage, TaskSendMessage
from cc.taskmgr import TaskManager
from skytools import DBScript
from cc.crypto import CryptoContext
from cc.stream import CCReqStream

import skytools

_usage = """
%prog [options] INI CMD [subcmd args]

Use-cases:

  List handler modules: %prog --list
  Show module details:  %prog --show
  Launch module:        %prog --send [--cc=URL] K=V [K=V ...]

Requered keys for '--send': handler=  host=
"""

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
        p.add_option("--list", action="store_true", help="list task modules")
        p.add_option("--show", action="store_true", help="show module details")
        p.add_option("--send", action="store_true", help="send task")
        p.set_usage(_usage.strip())
        return p

    def load_config(self):
        return skytools.Config(self.service_name, None,
            user_defs = {'use_skylog': '0', 'job_name': 'taskclient'})

    def startup(self):
        super(TaskClient, self).startup()

        self.cc_url = self.options.cc or 'tcp://127.0.0.1:15000'

        self.xtx = CryptoContext(self.cf, self.log)
        self.ccrq = CCReqStream(self.cc_url, self.xtx, self.log)
        self.taskmgr = TaskManager(self.ccrq, self.log)

    def work(self):
        self.set_single_loop(1)

        tid = str(uuid.uuid4())

        task = TaskSendMessage(
                req = 'task.send.' + tid,
                task_id = tid,
                task_host = 'hostname',
                task_handler = 'dsthandler')
        rep = self.taskmgr.send_task(task)
        self.log.info('reply: %r', rep.replies)


if __name__ == '__main__':
    script = TaskClient('taskclient', sys.argv[1:])
    script.start()

