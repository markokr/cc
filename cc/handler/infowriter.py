import os, os.path
import threading

import skytools
import zmq

import cc.util
from cc.handler import CCHandler
from cc.handler.proxy import BaseProxyHandler
from cc.message import CCMessage

__all__ = ['InfoWriter']

CC_HANDLER = 'InfoWriter'

#
# infofile writer master
#

comp_ext = {
    'gzip': '.gz',
    'bzip2': '.bz2',
    }

class InfoWriter (BaseProxyHandler):
    """ Simply writes to files (with help from workers) """

    CC_ROLES = ['remote']

    log = skytools.getLogger('h:InfoWriter')

    def startup (self):
        super(InfoWriter, self).startup()

        self.workers = []
        self.wparams = {} # passed to workers

        self.wparams['dstdir'] = self.cf.getfile ('dstdir')
        self.wparams['host_subdirs'] = self.cf.getbool ('host-subdirs', 0)
        self.wparams['bakext'] = self.cf.get ('bakext', '')
        self.wparams['write_compressed'] = self.cf.get ('write-compressed', '')
        assert self.wparams['write_compressed'] in [None, '', 'no', 'keep', 'yes']
        if self.wparams['write_compressed'] == 'yes':
            self.wparams['compression'] = self.cf.get ('compression', '')
            if self.wparams['compression'] not in ('gzip', 'bzip2'):
                self.log.error ("unsupported compression: %s", self.wparams['compression'])
            self.wparams['compression_level'] = self.cf.getint ('compression-level', '')

    def make_socket (self):
        """ Create socket for sending msgs to workers. """
        url = 'inproc://workers'
        sock = self.zctx.socket (zmq.XREQ)
        port = sock.bind_to_random_port (url)
        self.worker_url = "%s:%d" % (url, port)
        return sock

    def launch_workers (self):
        """ Create and start worker threads. """
        nw = self.cf.getint ('worker-threads', 10)
        for i in range (nw):
            wname = "%s.worker-%i" % (self.hname, i)
            self.log.info ("starting %s", wname)
            w = InfoWriter_Worker (wname, self.xtx, self.zctx, self.worker_url, self.wparams)
            w.stat_inc = self.stat_inc # XXX
            self.workers.append (w)
            w.start()

    def stop (self):
        """ Signal workers to shut down. """
        self.log.info ('stopping')
        for w in self.workers:
            self.log.info ("signalling %s", w.name)
            w.stop()

#
# infofile writer worker
#

class InfoWriter_Worker (threading.Thread):
    """ Simply writes to files. """

    log = skytools.getLogger ('h:InfoWriter_Worker')

    def __init__ (self, name, xtx, zctx, url, params = {}):
        super(InfoWriter_Worker, self).__init__(name=name)

        self.log = skytools.getLogger ('h:InfoWriter_Worker' + name[name.rfind('-'):])
        #self.log = skytools.getLogger (self.log.name + name[name.rfind('-'):])
        #self.log = skytools.getLogger (name)

        self.xtx = xtx
        self.zctx = zctx
        self.master_url = url

        for k, v in params.items():
            self.log.trace ("setattr: %s -> %r", k, v)
            setattr (self, k, v)

        self.looping = True

    def run (self):
        self.log.info ("%s running", self.name)
        self.master = self.zctx.socket (zmq.XREP)
        self.master.connect (self.master_url)
        while self.looping:
            try:
                self.work()
            except:
                self.log.exception ("worker crashed, dropping msg")
        self.shutdown()

    def work (self):
        zmsg = self.master.recv_multipart()
        try:
            cmsg = CCMessage (zmsg)
        except:
            self.log.exception ("invalid CC message")
        else:
            self.handle_msg (cmsg)

    def handle_msg (self, cmsg):
        """ Got message from master, process it. """

        data = cmsg.get_payload(self.xtx)
        if not data: return

        mtime = data['mtime']
        mode = data['mode']
        host = data['hostname']
        fn = os.path.basename(data['filename'])

        # sanitize
        host = host.replace('/', '_')
        if mode not in ['', 'b']:
            self.log.warning ("unsupported fopen mode (%r), ignoring it", mode)
            mode = 'b'

        # add file ext if needed
        if self.write_compressed == 'keep':
            if data['comp'] not in [None, '', 'none']:
                fn += comp_ext[data['comp']]
        elif self.write_compressed == 'yes':
            fn += comp_ext[self.compression]

        # decide destination file
        if self.host_subdirs:
            subdir = os.path.join(self.dstdir, host)
            dstfn = os.path.join(subdir, fn)
            if not os.path.isdir(subdir):
                os.mkdir(subdir)
        else:
            dstfn = os.path.join(self.dstdir, '%s--%s' % (host, fn))

        # check if file exists and is older
        try:
            st = os.stat(dstfn)
            if st.st_mtime == mtime:
                self.log.info('%s mtime matches, skipping', dstfn)
                return
            elif st.st_mtime > mtime:
                self.log.info('%s mtime newer, skipping', dstfn)
                return
        except OSError:
            pass

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
                body = cc.util.compress (deco, self.compression, {'level': self.compression_level})
                self.log.debug ("compressed from %i to %i", len(raw), len(body))
            else:
                body = raw

        # write file, apply original mtime
        self.log.debug ('writing %i bytes to %s', len(body), dstfn)
        cc.util.write_atomic (dstfn, body, bakext = self.bakext, mode = mode)
        os.utime(dstfn, (mtime, mtime))

        self.stat_inc ('written_bytes', len(body))
        self.stat_inc ('written_files')

    def stop (self):
        self.looping = False

    def shutdown (self):
        self.log.info ("%s stopping", self.name)
