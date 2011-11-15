import logging
import os
import time

from zmq.eventloop.ioloop import IOLoop, PeriodicCallback

from cc.handler import CCHandler

__all__ = ['TailWriter']

CC_HANDLER = 'TailWriter'

#
# logtail writer
#

class TailWriter (CCHandler):
    """ Simply appends to files """

    CC_ROLES = ['remote']

    log = logging.getLogger('h:TailWriter')

    def __init__ (self, hname, hcf, ccscript):
        super(TailWriter, self).__init__(hname, hcf, ccscript)

        self.dstdir = self.cf.getfile ('dstdir')
        self.host_subdirs = self.cf.getboolean ('host-subdirs', 0)
        self.maint_period = self.cf.getint ('maint-period', 3)
        self.files = {}

        self.ioloop = IOLoop.instance()
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

            fd = { 'obj': fobj, 'mode': mode, 'path': dstfn,
                   'ftime': time.time() }
            self.files[fi] = fd

        body = cmsg.get_part3() # blob
        if not body:
            body = data['data'].decode('base64')

        # append to file
        self.log.debug ('appending data to %s', fd['path'])
        fd['obj'].write (body)
        fd['wtime'] = time.time()

    def do_maint (self):
        """ Close long-open files; flush inactive files. """
        self.log.debug('')
        now = time.time()
        zombies = []
        for k, fd in self.files.iteritems():
            if now - fd['wtime'] > 30: # XXX: make configurable (also maxfiles)
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
        self.log.debug('')
        for fd in self.files.itervalues():
            fd['obj'].close()
            self.log.info ('closed %s', fd['path'])
