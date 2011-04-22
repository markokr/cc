#! /usr/bin/env python

""" HOTS bus - confdb bridge.
"""

import sys
import hots.script

class HotsDBServer(hots.script.HotsDBScript):

    def work(self):
        db = self.get_database('confdb', autocommit = 1)
        curs = db.cursor()
        sock = self.get_socket('local-rep-confdb')

        msg = sock.recv()
        self.log.info('REQUEST: %s' % msg)

        curs.execute("select hots.apiwrapper(%s)", [msg])
        res = curs.fetchone()[0]
        self.log.info('REPLY: %s' % res)

        sock.send(res)
        return 1

if __name__ == '__main__':
    s = HotsDBServer('hots-db', sys.argv[1:])
    s.start()

