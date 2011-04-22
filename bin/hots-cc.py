#! /usr/bin/env python

""" HOTS bus - confdb bridge.
"""

import sys
import zmq
from hots.script import HotsScript

class HotsCC(HotsScript):

    def startup(self):
        HotsScript.startup(self)

        self.local_logging = self.get_stream('local-sub-logging')
        self.local_confdb = self.get_stream('local-rep-confdb')
        self.remote_logging = self.get_stream('remote-pub-logging')
        self.remote_confdb = self.get_stream('remote-req-confdb')

        self.local_logging.on_recv(self.handle_log_recv)
        self.local_confdb.on_recv(self.handle_confdb_req)
        self.remote_confdb.on_recv(self.handle_confdb_rep)

        self.local_logging.setsockopt(zmq.SUBSCRIBE, '')

    def work(self):
        self.log.info('Starting ioloop')
        self.ioloop.start()
        return 1

    def handle_log_recv(self, req):
        self.log.info("LOG handle_log_recv: %s" % repr(req))
        self.remote_logging.send_multipart(req)

    def handle_confdb_rep(self, msg):
        self.log.info("LOG handle_confdb_rep: %s" % repr(req))

    def handle_confdb_req(self, msg):
        self.log.info("LOG handle_confdb_req: %s" % repr(req))


if __name__ == '__main__':
    s = HotsCC('hots-cc', sys.argv[1:])
    s.start()

