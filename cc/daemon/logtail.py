#! /usr/bin/env python

"""
Logfile tailer for rotated log files.
Supports 2 operating modes: classic, rotated.

Assumes that:
. All log files reside in the same directory.
. We can find last log file by sorting the file list alphabetically.

In classic mode:
. When log is switched, the tailer continues tailing from the next file.
. When the tailer is restarted, it continues tailing from saved position.

In rotated mode:
. When log is switched, the tailer continues tailing from reopened file.
"""

from __future__ import with_statement

import glob
import os
import re
import sys
import time

import skytools

import cc.util
from cc.daemon import CCDaemon
from cc.reqs import LogtailMessage


class LogfileTailer (CCDaemon):
    """ Logfile tailer for rotated log files """

    log = skytools.getLogger ('d:LogfileTailer')

    BUF_MINBYTES = 64 * 1024
    PROBESLEFT = 2 # number of retries after old log EOF and new log spotted

    def reload (self):
        super(LogfileTailer, self).reload()

        self.op_mode = self.cf.get ('operation-mode', '')
        if self.op_mode not in (None, '', 'classic', 'rotated'):
            self.log.error ("unknown operation-mode: %s", self.op_mode)

        self.logdir = self.cf.getfile ('logdir')
        if self.op_mode in (None, '', 'classic'):
            self.logmask = self.cf.get ('logmask')
        elif self.op_mode == 'rotated':
            self.logname = self.cf.get ('logname')
            if re.search ('\?|\*', self.logname):
                self.log.error ("wildcards in logname not supported: %s", self.logname)
            self.logmask = self.logname

        self.compression = self.cf.get ('compression', '')
        if self.compression not in (None, '', 'none', 'gzip', 'bzip2'):
            self.log.error ("unknown compression: %s", self.compression)
        self.compression_level = self.cf.getint ('compression-level', '')
        self.msg_suffix = self.cf.get ('msg-suffix', '')
        self.use_blob = self.cf.getboolean ('use-blob', False)
        self.lag_maxbytes = cc.util.hsize_to_bytes (self.cf.get ('lag-max-bytes', '0'))

        self.reverse_sort = False
        self.buf_maxbytes = cc.util.hsize_to_bytes (self.cf.get ('buffer-bytes', '0'))
        self.buf_maxlines = self.cf.getint ('buffer-lines', -1)
        self.buf_maxdelay = 1.0

        # compensate for our config class weakness
        if self.buf_maxbytes <= 0: self.buf_maxbytes = None
        if self.buf_maxlines < 0: self.buf_maxlines = None
        # set defaults if nothing found in config
        if self.buf_maxbytes is None and self.buf_maxlines is None:
            self.buf_maxbytes = 1024 * 1024

        if self.compression not in (None, '', 'none'):
            if self.buf_maxbytes < self.BUF_MINBYTES:
                self.log.info ("buffer-bytes too low, adjusting: %i -> %i", self.buf_maxbytes, self.BUF_MINBYTES)
                self.buf_maxbytes = self.BUF_MINBYTES

    def startup (self):
        super(LogfileTailer, self).startup()

        self.logfile = None # full path
        self.logf = None # file object
        self.logfpos = None # tell()
        self.probesleft = self.PROBESLEFT
        self.first = True
        self.tailed_files = 0
        self.tailed_bytes = 0
        self.buffer = []
        self.bufsize = 0
        self.bufseek = None
        self.saved_fpos = None
        self.logf_dev = self.logf_ino = None

        try:
            sfn = self.get_save_filename()
            with open (sfn, "r") as f:
                s = f.readline().split('\t', 1)
                self.logfile = s[1].strip()
                self.saved_fpos = int(s[0])
                self.log.info ("found saved state for %s", self.logfile)

            if self.op_mode == 'rotated':
                self.log.info ("cannot use saved state in this operation mode")
                self.logfile = self.saved_fpos = None

            lag = self.count_lag_bytes()
            if lag is not None:
                self.log.info ("currently lagging %i bytes behind", lag)
                if lag > self.lag_maxbytes:
                    self.log.warning ("lag too big, skipping")
                    self.logfile = self.saved_fpos = None
            else:
                self.log.warning ("cannot determine lag, skipping")
                self.logfile = self.saved_fpos = None
            os.unlink (sfn)
        except IOError:
            pass

    def count_lag_bytes (self):
        files = self.get_all_filenames()
        if self.logfile not in files or self.saved_fpos is None:
            return None
        lag = 0
        while True:
            fn = files.pop()
            st = os.stat(fn)
            if (fn == self.logfile):
                break
            lag += st.st_size
        lag += st.st_size - self.saved_fpos
        return lag

    def get_all_filenames (self):
        """ Return sorted list of all log file names """
        lfni = glob.iglob (os.path.join (self.logdir, self.logmask))
        lfns = sorted (lfni, reverse = self.reverse_sort)
        return lfns

    def get_last_filename (self):
        """ Return the name of latest log file """
        files = self.get_all_filenames()
        if files:
            return files[-1]
        return None

    def get_next_filename (self):
        """ Return the name of "next" log file """
        files = self.get_all_filenames()
        if not files:
            return None
        try:
            i = files.index (self.logfile)
            if not self.first:
                fn = files[i+1]
            else:
                fn = files[i]
        except ValueError:
            fn = files[-1]
        except IndexError:
            fn = files[i]
        return fn

    def get_save_filename (self):
        """ Return the name of save file """
        return os.path.splitext(self.pidfile)[0] + ".save"

    def is_new_file_available (self):
        if self.op_mode in (None, '', 'classic'):
            return (self.logfile != self.get_next_filename())
        elif self.op_mode == 'rotated':
            st = os.stat (self.logfile)
            return (st.st_dev != self.logf_dev or st.st_ino != self.logf_ino)
        else:
            raise ValueError ("unsupported mode of operation")

    def try_open_file (self, name):
        """ Try open log file; sleep a bit if unavailable. """
        if name:
            assert self.buffer == [] and self.bufsize == 0
            try:
                self.logf = open (name, 'rb')
                self.logfile = name
                self.logfpos = 0
                self.bufseek = 0
                self.send_stats() # better do it async me think (?)
                self.log.info ("Tailing %s", self.logfile)
                self.stat_inc ('tailed_files')
                self.tailed_files += 1
                self.probesleft = self.PROBESLEFT
                st = os.fstat (self.logf.fileno())
                self.logf_dev, self.logf_ino = st.st_dev, st.st_ino
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
                self.try_open_file (self.get_next_filename())
                continue

            if self.first:
                # seek to saved position or end of first file
                if self.saved_fpos:
                    self.logf.seek (self.saved_fpos, os.SEEK_SET)
                else:
                    self.logf.seek (0, os.SEEK_END)
                self.bufseek = self.logfpos = self.logf.tell()
                self.log.info ("started at file position %i", self.logfpos)
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
                if self.probesleft < self.PROBESLEFT:
                    self.log.info ("DEBUG: new data in old log (!)")
                continue

            # reset EOF condition for next attempt
            self.logf.seek (0, os.SEEK_CUR)

            if self.bufsize > 0 and self.compression in (None, '', 'none'):
                self.send_frag()
            elif self.is_new_file_available():
                if self.probesleft <= 0:
                    self.log.trace ("new log, closing old one")
                    self.send_frag()
                    self.logf.close()
                    self.logf = None
                else:
                    self.log.trace ("new log, still waiting for old one")
                    self.probesleft -= 1
                    time.sleep (0.1)
            else:
                self.log.trace ("waiting")
                time.sleep (0.1)

    def send_frag (self):
        if self.bufsize == 0:
            return
        start = time.time()
        if self.compression in (None, '', 'none'):
            buf = ''.join(self.buffer)
        else:
            buf = cc.util.compress (''.join(self.buffer), self.compression,
                                    {'level': self.compression_level})
            self.log.debug ("compressed from %i to %i", self.bufsize, len(buf))
        if self.use_blob:
            data = ''
            blob = buf
        else:
            data = buf.encode('base64')
            blob = None
        msg = LogtailMessage(
                filename = self.logfile,
                comp = self.compression,
                fpos = self.bufseek,
                data = data,
                op_mode = self.op_mode,
                st_dev = self.logf_dev,
                st_ino = self.logf_ino)
        if self.msg_suffix:
            msg.req += '.' + self.msg_suffix
        self.ccpublish (msg, blob)
        elapsed = time.time() - start
        self.log.debug ("sent %i bytes in %f s", len(buf), elapsed)
        self.stat_inc ('duration', elapsed) # json/base64/compress time, actual send happens async
        self.stat_inc ('count')
        self.stat_inc ('tailed_bytes', self.bufsize)
        self.bufseek += self.bufsize
        self.buffer = []
        self.bufsize = 0
        assert self.bufseek == self.logfpos

    def work (self):
        self.connect_cc()
        self.log.info ("Watching %s", os.path.join (self.logdir, self.logmask))
        try:
            self.tail()
        except (IOError, OSError), e:
            self.log.error ("%s", e)
        return 1

    def stop (self):
        super(LogfileTailer, self).stop()
        self.log.info ("stopping")
        if self.logf:
            with open (self.get_save_filename(), "w") as f:
                print >> f, "%i\t%s" % (self.bufseek, self.logfile)
                self.log.info ("saved offset %i for %s", self.bufseek, self.logfile)


if __name__ == '__main__':
    s = LogfileTailer ('logfile_tailer', sys.argv[1:])
    s.start()
