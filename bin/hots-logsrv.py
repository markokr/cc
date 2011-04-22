#! /usr/bin/env python

""" HOTS bus - logdb bridge.
"""

import sys
import zmq
import hots.script

class HotsLogServer(hots.script.HotsDBScript):

    def startup(self):
        hots.script.HotsDBScript.startup(self)

        # see all messages
        sub = self.get_socket('local-sub-logging')
        sub.setsockopt(zmq.SUBSCRIBE, '')
        self.log.info('Init done')

    def work(self):
        sub = self.get_socket('local-sub-logging')
        pub = self.get_socket('local-pub-logging')
        msg = sub.recv()
        self.log.info('LOG: %s' % msg)
        pub.send(msg)
        return 1

if __name__ == '__main__':
    s = HotsLogServer('hots-logsrv', sys.argv[1:])
    s.start()

