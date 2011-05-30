#! /usr/bin/env python

""" HOTS bus - confdb bridge.
"""

import sys
import hots.script

import glob
import stat
import os, os.path
import json

class InfofileCollector(hots.script.HotsScript):

    def reload(self):
        super(InfofileCollector, self).reload()

        self.infodir = self.cf.getfile('infodir', '/home/nagios')

    def startup(self):
        super(InfofileCollector, self).startup()

        # fn -> time
        self.infomap = {}

    def process_file(self, fn):
        f = open(fn, 'r')
        st = os.fstat(f.fileno())
        old = self.infomap.get(fn, 0)
        mtime = st.st_mtime
        if st.st_size > 0 and mtime > old:
            body = f.read()
            self.infomap[fn] = mtime
            self.log.info('Sending: %s' % fn)
            self.send_file(fn, body, mtime)
        f.close()

    def send_file(self, fn, body, mtime):
        jmsg = {
            'req': 'pub.infofile',
            'filename': os.path.basename(fn),
            'data': body,
        }
        hdr = 'pub.infofile'
        msg = json.dumps(jmsg)

        z = self.get_socket('remote-pub-infofile')
        z.send_multipart(['', hdr, msg])

    def work(self):
        fnlist = glob.glob(self.infodir + '/info.*')
        for fn in fnlist:
            self.process_file(fn)
        self.log.info('files:%d' % len(fnlist))

if __name__ == '__main__':
    s = InfofileCollector('infofile_collector', sys.argv[1:])
    s.start()

