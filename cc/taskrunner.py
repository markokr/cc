#! /usr/bin/env python

import sys, time
import zmq, zmq.eventloop

from zmq.eventloop.ioloop import PeriodicCallback

import skytools

from cc import json
from cc.message import CCMessage
from cc.stream import CCStream
from cc.handlers import cc_handler_lookup

class TaskRunner(skytools.BaseScript):
    """Register as handler for host.

    Receive and process tasks.
    """

    def startup(self):
        super(TaskRunner, self).startup()

        self.zctx = zmq.Context()
        self.ioloop = zmq.eventloop.IOLoop.instance()

        # initialize local listen socket
        s = self.zctx.socket(zmq.XREQ)
        s.setsockopt(zmq.LINGER, 500)
        s.connect(self.cf.get('cc-socket'))
        self.cc = CCStream(s, self.ioloop)
        self.cc.on_recv(self.handle_cc_recv)

        self.periodic_reg()

        self.maint_period = 15
        self.timer = PeriodicCallback(self.periodic_reg, self.maint_period*1000, self.ioloop)
        self.timer.start()

    def handle_cc_recv(self, zmsg):
        """Got task, do something with it"""
        self.log.info("LOG handle_cc_recv: %s", repr(zmsg))

    def work(self):
        """Default work loop simply runs ioloop."""
        self.log.info('Starting ioloop')
        self.ioloop.start()
        return 1

    def periodic_reg(self):
        """Register taskrunner in central router."""
        req = {'req': 'req.task.register', 'host': 'hostname'}
        zmsg = ['', req['req'], json.dumps(req)]
        self.log.info('maint: %s', repr(zmsg))
        self.cc.send_multipart(zmsg)

if __name__ == '__main__':
    s = TaskRunner('cctaskrunner', sys.argv[1:])
    s.start()

