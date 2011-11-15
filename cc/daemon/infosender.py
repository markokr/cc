#! /usr/bin/env python

"""Read infofiles.
"""

import glob
import logging
import os, os.path
import sys

import cc.util

from cc import json
from cc.daemon import CCDaemon
from cc.reqs import InfofileMessage

class InfoStamp:
    def __init__(self, fn, st):
        self.filename = fn
        self.filestat = st
        self.modified = 1
        self.checked = 0

    def check_send(self, st):
        if (st.st_mtime != self.filestat.st_mtime
                or st.st_size != self.filestat.st_size
                or st.st_size == 0):
            # st changed, new mod
            self.modified = 1
            self.filestat = st
            return 0
        elif self.modified:
            return 1
        else:
            return 0

class InfofileCollector(CCDaemon):

    log = logging.getLogger('d:InfofileCollector')

    def reload(self):
        super(InfofileCollector, self).reload()

        self.infodir = self.cf.getfile('infodir')
        self.infomask = self.cf.get('infomask')
        self.compression = self.cf.get ('compression', 'none')
        if self.compression not in (None, '', 'none', 'gzip', 'bzip2'):
            self.log.error ("unknown compression: %s", self.compression)
        self.compression_level = self.cf.getint ('compression-level', '')
        self.use_blob = self.cf.getboolean ('use-blob', False)

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
        cfb = cc.util.compress (body, self.compression, {'level': self.compression_level})
        self.log.debug ("file compressed from %i to %i", len(body), len(cfb))
        if self.use_blob:
            msg = InfofileMessage(
                    filename = os.path.basename(fs.filename),
                    mtime = fs.filestat.st_mtime,
                    comp = self.compression,
                    data = '')
            self.ccpublish (msg, cfb)
        else:
            msg = InfofileMessage(
                    filename = os.path.basename(fs.filename),
                    mtime = fs.filestat.st_mtime,
                    comp = self.compression,
                    data = cfb.encode('base64'))
            self.ccpublish (msg)

    def find_new(self):
        fnlist = glob.glob (os.path.join (self.infodir, self.infomask))
        newlist = []
        for fn in fnlist:
            try:
                st = os.stat(fn)
            except OSError, e:
                self.log.info('%s: %s', fn, e)
                continue
            if fn not in self.infomap:
                fstamp = InfoStamp(fn, st)
                self.infomap[fn] = fstamp
            else:
                old = self.infomap[fn]
                if old.check_send(st):
                    newlist.append(old)
        return newlist

    def work(self):
        self.connect_cc()
        newlist = self.find_new()
        for fs in newlist:
            try:
                self.process_file(fs)
            except (OSError, IOError), e:
                self.log.info('%s: %s', fs.filename, e)
        self.stat_inc('changes', len(newlist))

if __name__ == '__main__':
    s = InfofileCollector('infofile_collector', sys.argv[1:])
    s.start()
