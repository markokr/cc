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
import os
import sys
import time

from cc.daemon import CCDaemon
from cc.reqs import LogtailMessage


class LogTail (object):
    def __init__ (self, logdir, logmask, logger, reverse_sort = False):
        self.log = logger
        self.logdir = logdir
        self.logmask = logmask
        self.reverse_sort = reverse_sort
        self.logfile = None # full path
        self.logname = None # file name
        self.logf = None # file object
        self.probesleft = 2
        self.first = True
        self.tailed_files = 0
        self.tailed_bytes = 0

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

    def readline (self):
        """ Read single line from log file, switch to next file if current file is exhausted.
        """
        while True:
            # If not already open, keep trying until it becomes available
            while not self.logf:
                self.logfile = self.get_last_filename()
                if self.logfile:
                    try:
                        self.logf = open (self.logfile)
                        self.log.info ("Tailing %s", self.logfile)
                        self.logname = os.path.basename (self.logfile)
                        self.tailed_files += 1
                        self.probesleft = 2
                    except IOError, e:
                        time.sleep (0.2)
                else:
                    time.sleep (0.2)

            if self.first:
                # seek to end of first file
                self.logf.seek (0, os.SEEK_END)
                self.first = False

            line = self.logf.readline()
            if line:
                self.tailed_bytes += len(line)
                return line

            # reset EOF condition for next attempt
            self.logf.seek (0, os.SEEK_CUR)

            if self.logfile != self.get_last_filename():
                self.probesleft -= 1 # what for? and no wait needed?
                if self.probesleft <= 0:
                    self.logf = None
            else:
                time.sleep (0.2)

    def __iter__ (self):
        """ Initialize iterator """
        return self

    def next (self):
        """ Iterator wrapper for readline() """
        return self.readline()


class LogfileTailer (CCDaemon):

    def reload (self):
        super(LogfileTailer, self).reload()

        self.logdir = self.cf.getfile ('logdir')
        self.logmask = self.cf.get ('logmask')

    def send_frag (self, fname, frag):
        msg = LogtailMessage(
                filename = fname,
                data = frag.encode('base64'))
        self.ccpublish (msg)

    def tail (self):
        self.log.info ("Watching %s", os.path.join (self.logdir, self.logmask))
        logtail = LogTail (self.logdir, self.logmask, logger = self.log)
        try:
            for line in logtail:
                self.send_frag (logtail.logname, line)
                if self.last_sigint:
                    break
        except (IOError, OSError), e:
            self.log.warn ("%s", e)
        self.stat_inc ('tailed_files', logtail.tailed_files)
        self.stat_inc ('tailed_bytes', logtail.tailed_bytes)

    def work (self):
        self.connect_cc()
        self.tail()
        return 1


if __name__ == '__main__':
    s = LogfileTailer ('logfile_tailer', sys.argv[1:])
    s.start()
