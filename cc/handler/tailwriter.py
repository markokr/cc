import datetime
import os
import threading
import time
from collections import deque

import skytools
import zmq
from zmq.eventloop.ioloop import PeriodicCallback

import cc.util
from cc.handler import CCHandler
from cc.message import CCMessage
from cc.reqs import ReplyMessage
from cc.stream import CCStream

__all__ = ['TailWriter']

CC_HANDLER = 'TailWriter'

#
# logtail writer master
#

BUF_MINBYTES = 64 * 1024
DATETIME_SUFFIX = ".%Y-%m-%d_%H-%M-%S"
FLUSH_DELAY = 3     # since last write
CLOSE_DELAY = 30    # since last write

comp_ext = {
    'gzip': '.gz',
    'bzip2': '.bz2',
    }

class FileState (object):
    """ File tracking state (master) """
    __slots__ = ('ident', 'wname', 'waddr', 'queue', 'count', 'ctime', 'atime')

    def __init__ (self, ident, count=0):
        self.atime = self.ctime = time.time()
        self.queue = deque()    # msgs yet to send
        self.count = count      # sent and not ack'd
        self.waddr = None       # socket identity
        self.wname = None       # thread name
        self.ident = ident

    def send_to (self, sock):
        while self.queue:
            self.count += 1
            m = self.queue.popleft()
            m.set_route ([self.waddr])
            m.send_to (sock)
        self.atime = time.time()

class TailWriter (CCHandler):
    """ Simply appends to files (with help from workers) """

    CC_ROLES = ['remote']

    log = skytools.getLogger ('h:TailWriter')

    def __init__ (self, hname, hcf, ccscript):
        super(TailWriter, self).__init__(hname, hcf, ccscript)

        self.files = {}
        self.workers = []
        self.wparams = {} # passed to workers

        self.wparams['dstdir'] = self.cf.getfile ('dstdir')
        self.wparams['host_subdirs'] = self.cf.getbool ('host-subdirs', 0)
        self.wparams['maint_period'] = self.cf.getint ('maint-period', 3)
        self.wparams['write_compressed'] = self.cf.get ('write-compressed', '')
        assert self.wparams['write_compressed'] in [None, '', 'no', 'keep', 'yes']
        if self.wparams['write_compressed'] in ('keep', 'yes'):
            self.log.info ("position checking not supported for compressed files")
        if self.wparams['write_compressed'] == 'yes':
            self.wparams['compression'] = self.cf.get ('compression', '')
            if self.wparams['compression'] not in ('gzip', 'bzip2'):
                self.log.error ("unsupported compression: %s", self.wparams['compression'])
            self.wparams['compression_level'] = self.cf.getint ('compression-level', '')
            self.wparams['buf_maxbytes'] = cc.util.hsize_to_bytes (self.cf.get ('buffer-bytes', '1 MB'))
            if self.wparams['buf_maxbytes'] < BUF_MINBYTES:
                self.log.info ("buffer-bytes too low, adjusting: %i -> %i", self.wparams['buf_maxbytes'], BUF_MINBYTES)
                self.wparams['buf_maxbytes'] = BUF_MINBYTES

        # initialise sockets for communication with workers
        self.dealer_stream, self.dealer_url = self.init_comm (zmq.XREQ, 'inproc://workers-dealer', self.dealer_on_recv)
        self.router_stream, self.router_url = self.init_comm (zmq.XREP, 'inproc://workers-router', self.router_on_recv)

        self.launch_workers()

        self.timer_maint = PeriodicCallback (self.do_maint, self.wparams['maint_period'] * 1000, self.ioloop)
        self.timer_maint.start()

    def init_comm (self, stype, url, cb):
        """ Create socket, stream, etc for communication with workers. """
        sock = self.zctx.socket (stype)
        port = sock.bind_to_random_port (url)
        curl = "%s:%d" % (url, port)
        stream = CCStream (sock, self.ioloop)
        stream.on_recv (cb)
        return (stream, curl)

    def launch_workers (self):
        """ Create and start worker threads. """
        nw = self.cf.getint ('worker-threads', 10)
        for i in range (nw):
            wname = "%s.worker-%i" % (self.hname, i)
            self.log.info ("starting %s", wname)
            w = TailWriter_Worker(
                    wname, self.xtx, self.zctx, self.ioloop,
                    self.dealer_url, self.router_url, self.wparams)
            w.stat_inc = self.stat_inc # XXX
            self.workers.append (w)
            w.start()

    def handle_msg (self, cmsg):
        """ Got message from client, process it. """

        data = cmsg.get_payload (self.xtx)
        if not data: return

        host = data['hostname']
        fn = data['filename']
        st_dev = data.get('st_dev')
        st_ino = data.get('st_ino')

        fi = (host, st_dev, st_ino, fn)
        if fi in self.files:
            fd = self.files[fi]
            if fd.waddr: # already accepted ?
                self.log.trace ("passing %r to %s", fn, fd.wname)
                fd.queue.append (cmsg)
                fd.send_to (self.router_stream)
            else:
                self.log.trace ("queueing %r", fn)
                fd.queue.append (cmsg)
        else:
            fd = FileState (fi, 1)
            self.files[fi] = fd
            self.log.trace ("offering %r", fn)
            self.dealer_stream.send_cmsg (cmsg)

    def dealer_on_recv (self, zmsg):
        """ Got reply from worker via "dealer" connection """
        self.log.warning ("reply via dealer: %s", zmsg)

    def router_on_recv (self, zmsg):
        """ Got reply from worker via "router" connection """
        cmsg = CCMessage (zmsg)
        data = cmsg.get_payload (self.xtx)
        fi = (data['d_hostname'], data['d_st_dev'], data['d_st_ino'], data['d_filename'])
        fd = self.files[fi]
        if fd.waddr is None:
            fd.waddr = zmsg[0]
            fd.wname = data['worker']
        else:
            assert fd.waddr == zmsg[0] and fd.wname == data['worker']
        fd.atime = time.time()
        fd.count -= 1
        assert fd.count >= 0

    def do_maint (self):
        """ Check & flush queues; drop inactive files. """
        self.log.trace ('cleanup')
        now = time.time()
        zombies = []
        for k, fd in self.files.iteritems():
            if fd.queue and fd.waddr:
                self.log.trace ("passing %r to %s", fd.ident, fd.wname)
                fd.send_to (self.router_stream)
            if (fd.count == 0) and (now - fd.atime > 2 * CLOSE_DELAY): # you'd better use msg for this
                self.log.debug ("forgetting %r", fd.ident)
                zombies.append(k)
        for k in zombies:
                self.files.pop(k)

    def stop (self):
        """ Signal workers to shut down. """
        super(TailWriter, self).stop()
        self.log.info ('stopping')
        self.timer_maint.stop()
        for w in self.workers:
            self.log.info ("signalling %s", w.name)
            w.stop()

#
# logtail writer worker
#

class TailWriter_Worker (threading.Thread):
    """ Simply appends to files """

    log = skytools.getLogger ('h:TailWriter_Worker')

    def __init__ (self, name, xtx, zctx, ioloop, dealer_url, router_url, params = {}):
        super(TailWriter_Worker, self).__init__(name=name)

        self.log = skytools.getLogger ('h:TailWriter_Worker' + name[name.rfind('-'):])
        #self.log = skytools.getLogger (self.log.name + name[name.rfind('-'):])
        #self.log = skytools.getLogger (name)

        self.xtx = xtx
        self.zctx = zctx
        self.ioloop = ioloop
        self.shared_url = dealer_url
        self.direct_url = router_url

        for k, v in params.items():
            self.log.trace ("setattr: %s -> %r", k, v)
            setattr (self, k, v)

        self.files = {}
        self.looping = True

    def startup (self):
        # announce channel (for new files)
        self.sconn = self.zctx.socket (zmq.XREP)
        self.sconn.connect (self.shared_url)
        # direct channel (for grabbed files)
        self.dconn = self.zctx.socket (zmq.XREQ)
        self.dconn.connect (self.direct_url)
        # polling interface
        self.poller = zmq.Poller()
        self.poller.register (self.sconn, zmq.POLLIN)
        self.poller.register (self.dconn, zmq.POLLIN)
        # schedule regular maintenance
        self.timer_maint = PeriodicCallback (self.do_maint, self.maint_period * 1000, self.ioloop)
        self.timer_maint.start()

    def run (self):
        self.log.info ("%s running", self.name)
        self.startup()
        while self.looping:
            try:
                self.work()
            except:
                self.log.exception ("worker crashed, dropping msg")
        self.shutdown()

    def work (self):
        socks = dict (self.poller.poll (1000))
        if self.dconn in socks and socks[self.dconn] == zmq.POLLIN:
            zmsg = self.dconn.recv_multipart()
        elif self.sconn in socks and socks[self.sconn] == zmq.POLLIN:
            zmsg = self.sconn.recv_multipart()
        else: # timeout
            return
        try:
            cmsg = CCMessage (zmsg)
        except:
            self.log.exception ("invalid CC message")
        else:
            self.handle_msg (cmsg)

    def handle_msg (self, cmsg):
        """ Got message from master, process it. """

        data = cmsg.get_payload (self.xtx)
        if not data: return

        mode = data['mode']
        host = data['hostname']
        fn = os.path.basename (data['filename'])
        op_mode = data.get('op_mode')
        st_dev = data.get('st_dev')
        st_ino = data.get('st_ino')

        # let master know asap :-)
        self._send_ack (host, st_dev, st_ino, data['filename'], data.get('fpos'))

        # sanitize
        host = host.replace ('/', '_')
        if mode not in ['', 'b']:
            self.log.warning ("unsupported fopen mode (%r), ignoring it", mode)
            mode = 'b'
        if op_mode not in [None, '', 'classic', 'rotated']:
            self.log.warning ("unsupported operation mode (%r), ignoring it", op_mode)
            op_mode = None

        # add file ext if needed
        if self.write_compressed == 'keep':
            if data['comp'] not in [None, '', 'none']:
                fn += comp_ext[data['comp']]
        elif self.write_compressed == 'yes':
            fn += comp_ext[self.compression]

        # Cache open files
        fi = (host, st_dev, st_ino, fn)
        if fi in self.files:
            fd = self.files[fi]
            if mode != fd['mode']:
                self.log.error ("fopen mode mismatch (%s -> %s)", mode, fd['mode'])
                return
            if op_mode != fd['op_mode']:
                self.log.error ("operation mode mismatch (%s -> %s)", op_mode, fd['op_mode'])
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
            if op_mode == 'rotated':
                dt = datetime.datetime.today()
                dstfn += dt.strftime (DATETIME_SUFFIX)

            fobj = open (dstfn, 'a' + mode)
            self.log.info ('opened %s', dstfn)

            now = time.time()
            fd = { 'obj': fobj, 'mode': mode, 'path': dstfn,
                   'wtime': now, 'ftime': now, 'buf': [], 'bufsize': 0,
                   'offset': 0, 'op_mode': op_mode }
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

        if hasattr (data, 'fpos') and (self.write_compressed in [None, '', 'no']
                or (self.write_compressed == 'keep' and data['comp'] in [None, '', 'none'])):
            fpos = fd['obj'].tell()
            if data['fpos'] != fpos + fd['offset']:
                self.log.warning ("sync lost: %i -> %i", fpos, data['fpos'])
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

    def _send_ack (self, hostname, st_dev, st_ino, filename, fpos):
        """ Send ack to master """
        rep = ReplyMessage(
                worker = self.name,
                d_hostname = hostname,
                d_st_dev = st_dev,
                d_st_ino = st_ino,
                d_filename = filename,
                d_fpos = fpos)
        rcm = self.xtx.create_cmsg (rep)
        rcm.send_to (self.dconn)

    def do_maint (self):
        """ Close long-open files; flush inactive files. """
        self.log.trace ('cleanup')
        now = time.time()
        zombies = []
        for k, fd in self.files.iteritems():
            if now - fd['wtime'] > CLOSE_DELAY:
                if fd['buf']:
                    body = self._process_buffer(fd)
                    fd['obj'].write(body)
                fd['obj'].close()
                self.log.info ('closed %s', fd['path'])
                zombies.append(k)
            elif (fd['wtime'] > fd['ftime']) and (now - fd['wtime'] > FLUSH_DELAY):
                # note: think about small writes within flush period
                fd['obj'].flush()
                self.log.debug ('flushed %s', fd['path'])
                fd['ftime'] = now
        for k in zombies:
                self.files.pop(k)

    def stop (self):
        self.looping = False

    def shutdown (self):
        """ Close all open files """
        self.log.info ('%s stopping', self.name)
        for fd in self.files.itervalues():
            if fd['buf']:
                body = self._process_buffer(fd)
                fd['obj'].write(body)
            fd['obj'].close()
            self.log.info ('closed %s', fd['path'])
