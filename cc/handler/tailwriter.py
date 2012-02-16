import os
import time

import skytools
from zmq.eventloop.ioloop import PeriodicCallback

import cc.util
from cc.handler import CCHandler

__all__ = ['TailWriter']

CC_HANDLER = 'TailWriter'

#
# logtail writer
#

BUF_MINBYTES = 64 * 1024

comp_ext = {
    'gzip': '.gz',
    'bzip2': '.bz2',
    }

class TailWriter (CCHandler):
    """ Simply appends to files """

    CC_ROLES = ['remote']

    log = skytools.getLogger ('h:TailWriter')

    def __init__ (self, hname, hcf, ccscript):
        super(TailWriter, self).__init__(hname, hcf, ccscript)

        self.dstdir = self.cf.getfile ('dstdir')
        self.host_subdirs = self.cf.getboolean ('host-subdirs', 0)
        self.maint_period = self.cf.getint ('maint-period', 3)
        self.files = {}

        self.write_compressed = self.cf.get ('write-compressed', '')
        assert self.write_compressed in [None, '', 'no', 'keep', 'yes']
        if self.write_compressed == 'yes':
            self.compression = self.cf.get ('compression', '')
            if self.compression not in ('gzip', 'bzip2'):
                self.log.error ("unsupported compression: %s", self.compression)
            self.compression_level = self.cf.getint ('compression-level', '')
            self.buf_maxbytes = cc.util.hsize_to_bytes (self.cf.get ('buffer-bytes', 1024 * 1024))
            if self.buf_maxbytes < BUF_MINBYTES:
                self.log.info ("buffer-bytes too low, adjusting: %i -> %i", self.buf_maxbytes, BUF_MINBYTES)
                self.buf_maxbytes = BUF_MINBYTES

        self.timer_maint = PeriodicCallback (self.do_maint, self.maint_period * 1000, self.ioloop)
        self.timer_maint.start()

    def handle_msg (self, cmsg):
        """ Got message from client, process it. """

        data = cmsg.get_payload (self.xtx)
        if not data: return

        mode = data['mode']
        host = data['hostname']
        fn = os.path.basename (data['filename'])

        # sanitize
        host = host.replace ('/', '_')
        if mode not in ['', 'b']:
            self.log.warn ("unsupported fopen mode ('%s'), ignoring it", mode)
            mode = 'b'

        # add file ext if needed
        if self.write_compressed == 'keep':
            if data['comp'] not in [None, '', 'none']:
                fn += comp_ext[data['comp']]
        elif self.write_compressed == 'yes':
            fn += comp_ext[self.compression]

        # Cache open files
        fi = (host, fn)
        if fi in self.files:
            fd = self.files[fi]
            if mode != fd['mode']:
                self.log.error ("fopen mode mismatch (%s -> %s)", mode, fd['mode'])
                return
        else:
            # decide destination file
            if self.host_subdirs:
                subdir = os.path.join (self.dstdir, host)
                dstfn = os.path.join (subdir, fn)
                if not os.path.isdir (subdir):
                    os.mkdir (subdir)
            else:
                dstfn = os.path.join (self.dstdir, '%s--%s' % (host, fn))

            fobj = open (dstfn, 'a' + mode)
            self.log.info ('opened %s', dstfn)

            now = time.time()
            fd = { 'obj': fobj, 'mode': mode, 'path': dstfn,
                   'wtime': now, 'ftime': now, 'buf': [], 'bufsize': 0,
                   'offset': 0 }
            self.files[fi] = fd

        raw = cmsg.get_part3() # blob
        if not raw:
            raw = data['data'].decode('base64')

        if self.write_compressed in [None, '', 'no']:
            if data['comp'] not in (None, '', 'none'):
                body = cc.util.decompress (raw, data['comp'])
                self.log.debug ("decompressed from %i to %i", len(raw), len(body))
            else:
                body = raw
        elif self.write_compressed == 'keep':
            body = raw
        elif self.write_compressed == 'yes':
            if (data['comp'] != self.compression):
                deco = cc.util.decompress (raw, data['comp'])
                fd['buf'].append(deco)
                fd['bufsize'] += len(deco)
                if fd['bufsize'] < self.buf_maxbytes:
                    return
                body = self._process_buffer(fd)
            else:
                body = raw

        if hasattr (data, 'fpos'):
            fpos = fd['obj'].tell()
            if data['fpos'] != fpos + fd['offset']:
                self.log.warn ("sync lost: %i -> %i", fpos, data['fpos'])
                fd['offset'] = data['fpos'] - fpos

        # append to file
        self.log.debug ('appending %i bytes to %s', len(body), fd['path'])
        fd['obj'].write (body)
        fd['wtime'] = time.time()

        self.stat_inc ('appended_bytes', len(body))

    def _process_buffer (self, fd):
        """ Compress and reset write buffer """
        buf = ''.join(fd['buf'])
        out = cc.util.compress (buf, self.compression, {'level': self.compression_level})
        self.log.debug ("compressed from %i to %i", fd['bufsize'], len(out))
        fd['buf'] = []
        fd['bufsize'] = 0
        return out

    def do_maint (self):
        """ Close long-open files; flush inactive files. """
        self.log.trace ('cleanup')
        now = time.time()
        zombies = []
        for k, fd in self.files.iteritems():
            if now - fd['wtime'] > 30: # XXX: make configurable (also maxfiles)
                if fd['buf']:
                    body = self._process_buffer(fd)
                    fd['obj'].write(body)
                fd['obj'].close()
                self.log.info ('closed %s', fd['path'])
                zombies.append(k)
            elif (fd['wtime'] > fd['ftime']) and (now - fd['wtime'] > 3): # XXX: make configurable ?
                # note: think about small writes within flush period
                fd['obj'].flush()
                self.log.debug ('flushed %s', fd['path'])
                fd['ftime'] = now
        for k in zombies:
                self.files.pop(k)

    def stop (self):
        """ Close all open files """
        self.log.info ('stopping')
        for fd in self.files.itervalues():
            if fd['buf']:
                body = self._process_buffer(fd)
                fd['obj'].write(body)
            fd['obj'].close()
            self.log.info ('closed %s', fd['path'])
