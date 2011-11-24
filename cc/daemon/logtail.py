#! /usr/bin/env python

"""
Logfile tailer for rotated log files.

Assumes that:
. All log files reside in the same directory.
. We can find last log file by sorting the file list alphabetically.
. When log is switched, we start tailing from the last file - assuming
  there will be no gaps (quick processing on sufficiently large files).
"""

import glob
import logging
import os
import sys
import time

from cc.daemon import CCDaemon
from cc.reqs import LogtailMessage


class LogfileTailer (CCDaemon):
    """ Logfile tailer for rotated log files """

    log = logging.getLogger ('d:LogfileTailer')

    def reload (self):
        super(LogfileTailer, self).reload()

        self.logdir = self.cf.getfile ('logdir')
        self.logmask = self.cf.get ('logmask')
        self.use_blob = self.cf.getboolean ('use-blob', False)
        self.reverse_sort = False
        self.buf_maxbytes = self.cf.getint ('buffer-bytes', -1)
        self.buf_maxlines = self.cf.getint ('buffer-lines', -1)
        self.buf_maxdelay = 1.0

        # compensate for our config class weakness
        if self.buf_maxbytes < 0: self.buf_maxbytes = None
        if self.buf_maxlines < 0: self.buf_maxlines = None
        # set defaults if nothing found in config
        if self.buf_maxbytes is None and self.buf_maxlines is None:
            self.buf_maxbytes = 1024 * 1024

    def startup (self):
        super(LogfileTailer, self).startup()

        self.logfile = None # full path
        self.logf = None # file object
        self.logfpos = None # tell()
        self.probesleft = 2
        self.first = True
        self.tailed_files = 0
        self.tailed_bytes = 0
        self.buffer = []
        self.bufsize = 0

    def get_all_filenames (self):
        """ Return sorted list of all log file names """
        lfni = glob.iglob (os.path.join (self.logdir, self.logmask))
        lfns = sorted (lfni, reverse = self.reverse_sort)
        return lfns

    def get_last_filename (self):
        """ Return the name of current log file """
        files = self.get_all_filenames()
        if files:
            return files[-1]
        return None

    def try_open_file (self, name):
        """ Try open log file; sleep a bit if unavailable. """
        if name:
            try:
                self.logf = open (name, 'rb')
                self.logfile = name
                self.logfpos = 0
                self.send_stats() # better do it async me think (?)
                self.log.info ("Tailing %s", self.logfile)
                self.stat_inc ('tailed_files')
                self.tailed_files += 1
                self.probesleft = 2
            except IOError, e:
                self.log.info ("%s", e)
                time.sleep (0.2)
        else:
            self.log.debug ("no logfile available, waiting")
            time.sleep (0.2)

    def tail (self):
        """ Keep reading from log file (line by line), switch to next file if current file is exhausted.
        """
        while not self.last_sigint:
            if not self.logf:
                # if not already open, keep trying until it becomes available
                self.try_open_file (self.get_last_filename())
                continue

            if self.first:
                # seek to end of first file
                self.logf.seek (0, os.SEEK_END)
                self.first = False

            line = self.logf.readline()
            if line:
                s = len(line)
                self.logfpos += s
                self.tailed_bytes += s
                self.buffer.append(line)
                self.bufsize += s
                if (self.buf_maxbytes is not None and self.bufsize >= self.buf_maxbytes) or \
                        (self.buf_maxlines is not None and len(self.buffer) >= self.buf_maxlines):
                    self.send_frag()
                continue

            # reset EOF condition for next attempt
            self.logf.seek (0, os.SEEK_CUR)

            if self.bufsize > 0:
                self.send_frag()
            elif self.logfile != self.get_last_filename():
                if self.probesleft <= 0:
                    self.send_frag()
                    self.logf.close()
                    self.logf = None
                else:
                    self.probesleft -= 1
                    time.sleep (0.1)
            else:
                time.sleep (0.1)

    def send_frag (self):
        if self.bufsize == 0:
            return
        now = time.time()
        buf = ''.join(self.buffer)
        if self.use_blob:
            msg = LogtailMessage(
                    filename = self.logfile,
                    data = '')
            self.ccpublish (msg, buf)
        else:
            msg = LogtailMessage(
                    filename = self.logfile,
                    data = buf.encode('base64'))
            self.ccpublish (msg)
        self.log.debug ("sent %i bytes in %f s", self.bufsize, now - time.time())
        self.stat_inc ('tailed_bytes', self.bufsize)
        self.buffer = []
        self.bufsize = 0

    def work (self):
        self.connect_cc()
        self.log.info ("Watching %s", os.path.join (self.logdir, self.logmask))
        try:
            self.tail()
        except (IOError, OSError), e:
            self.log.error ("%s", e)
        return 1


if __name__ == '__main__':
    s = LogfileTailer ('logfile_tailer', sys.argv[1:])
    s.start()
