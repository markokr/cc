#! /usr/bin/env python

"""Read infofiles.
"""

import sys
import glob
import stat
import os, os.path
import time
import socket

from cc import json
from cc.job import CCJob

class InfoStamp:
    def __init__(self, fn, st):
        self.filename = fn
        self.st = st
        self.modified = 1
        self.checked = 0

    def check_send(self, st):
        if (st.st_mtime != self.st.st_mtime
                or st.st_size != self.st.st_size
                or st.st_size == 0):
            # st changed, new mod
            self.modified = 1
            self.st = st
            return 0
        elif self.modified:
            return 1
        else:
            return 0

class InfofileCollector(CCJob):

    def reload(self):
        super(InfofileCollector, self).reload()

        self.infodir = self.cf.getfile('infodir')

    def startup(self):
        super(InfofileCollector, self).startup()

        # fn -> stamp
        self.infomap = {}

    def process_file(self, fs):
        f = open(fs.filename, 'rb')
        st = os.fstat(f.fileno())
        if fs.check_send(st):
            body = f.read()
            if len(body) != st.st_size:
                return
            fs.modified = 0
            self.log.debug('Sending: %s', fs.filename)
            self.send_file(fs, body)
            self.stat_inc('count')
        f.close()

    def send_file(self, fs, body):
        jmsg = {
            'req': 'pub.infofile',
            'filename': os.path.basename(fs.filename),
            'hostname': self.hostname,
            'mtime': fs.st.st_mtime,
            'data': body,
        }
        hdr = 'pub.infofile'
        msg = json.dumps(jmsg)

        self.cc.send_multipart(['', hdr, msg])

    def find_new(self):
        fnlist = glob.glob(self.infodir + '/info.*')
        newlist = []
        for fn in fnlist:
            st = os.stat(fn)
            if fn not in self.infomap:
                fstamp = InfoStamp(fn, st)
                self.infomap[fn] = fstamp
            else:
                old = self.infomap[fn]
                if old.check_send(st):
                    newlist.append(old)
        return newlist

    def work(self):
        self.hostname = socket.gethostname()
        self.connect_cc()
        newlist = self.find_new()
        for fs in newlist:
            self.process_file(fs)
        self.stat_inc('changes', len(newlist))

if __name__ == '__main__':
    s = InfofileCollector('infofile_collector', sys.argv[1:])
    s.start()

