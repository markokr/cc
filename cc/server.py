#! /usr/bin/env python

"""Command-and-Control server.

client <-> ccserver|handler <-> handlerproc


"""


import errno
import os.path
import sys
import time

import skytools
import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback

from cc import __version__
from cc.crypto import CryptoContext
from cc.handler import cc_handler_lookup
from cc.message import CCMessage
from cc.stream import CCStream
from cc.util import hsize_to_bytes, reset_stats


LOG = skytools.dbdict(
    fmt = '%(asctime)s %(process)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
    datefmt = '',
    fmt_v = '%(asctime)s,%(msecs)03d %(levelname)s %(name)s.%(funcName)s: %(message)s',
    datefmt_v = '%H:%M:%S',
)
for k in LOG:
    LOG[k] = LOG[k].replace('%', '%%')


class CCServer(skytools.BaseScript):
    """Listens on single ZMQ sockets, dispatches messages to handlers.

    Config::
        ## Parameters for CCServer ##

        # listening socket for this CC instance
        cc-socket = tcp://127.0.0.1:22632

        # zmq customization:
        #zmq_nthreads = 1
        #zmq_linger = 500
        #zmq_hwm = 100

        #zmq_tcp_keepalive = 1
        #zmq_tcp_keepalive_intvl = 15
        #zmq_tcp_keepalive_idle = 240
        #zmq_tcp_keepalive_cnt = 4
    """
    extra_ini = """
    Extra segments::

        # map req prefix to handler segment
        [routes]
        log = h:locallog

        # segment for specific handler
        [h:locallog]
        handler = cc.handler.locallogger
    """

    log = skytools.getLogger('CCServer')

    cf_defaults = {
        'logfmt_console': LOG.fmt,
        'logfmt_file': LOG.fmt,
        'logfmt_console_verbose': LOG.fmt_v,
        'logfmt_file_verbose': LOG.fmt_v,
        'logdatefmt_console': LOG.datefmt,
        'logdatefmt_file': LOG.datefmt,
        'logdatefmt_console_verbose': LOG.datefmt_v,
        'logdatefmt_file_verbose': LOG.datefmt_v,
    }

    __version__ = __version__

    zmq_nthreads = 1
    zmq_linger = 500
    zmq_hwm = 100
    zmq_rcvbuf = 0 # means no change
    zmq_sndbuf = 0 # means no change

    zmq_tcp_keepalive = 1
    zmq_tcp_keepalive_intvl = 15
    zmq_tcp_keepalive_idle = 4*60
    zmq_tcp_keepalive_cnt = 4

    def reload(self):
        super(CCServer, self).reload()

        self.zmq_nthreads = self.cf.getint('zmq_nthreads', self.zmq_nthreads)
        self.zmq_hwm = self.cf.getint('zmq_hwm', self.zmq_hwm)
        self.zmq_linger = self.cf.getint('zmq_linger', self.zmq_linger)
        self.zmq_rcvbuf = hsize_to_bytes (self.cf.get ('zmq_rcvbuf', str(self.zmq_rcvbuf)))
        self.zmq_sndbuf = hsize_to_bytes (self.cf.get ('zmq_sndbuf', str(self.zmq_sndbuf)))

        self.zmq_tcp_keepalive = self.cf.getint ('zmq_tcp_keepalive', self.zmq_tcp_keepalive)
        self.zmq_tcp_keepalive_intvl = self.cf.getint ('zmq_tcp_keepalive_intvl', self.zmq_tcp_keepalive_intvl)
        self.zmq_tcp_keepalive_idle = self.cf.getint ('zmq_tcp_keepalive_idle', self.zmq_tcp_keepalive_idle)
        self.zmq_tcp_keepalive_cnt = self.cf.getint ('zmq_tcp_keepalive_cnt', self.zmq_tcp_keepalive_cnt)

    def print_ini(self):
        super(CCServer, self).print_ini()

        self._print_ini_frag(self.extra_ini)

    def startup(self):
        """Setup sockets and handlers."""

        super(CCServer, self).startup()

        self.log.info ("C&C server version %s starting up..", self.__version__)

        self.xtx = CryptoContext(self.cf)
        self.zctx = zmq.Context(self.zmq_nthreads)
        self.ioloop = IOLoop.instance()

        self.local_url = self.cf.get('cc-socket')

        self.cur_role = self.cf.get('cc-role', 'insecure')
        if self.cur_role == 'insecure':
            self.log.warning('CC is running in insecure mode, please add "cc-role = local" or "cc-role = remote" option to config')

        self.stat_level = self.cf.getint ('cc-stats', 1)
        if self.stat_level < 1:
            self.log.warning ('CC statistics level too low: %d', self.stat_level)

        # initialize local listen socket
        s = self.zctx.socket(zmq.XREP)
        s.setsockopt(zmq.LINGER, self.zmq_linger)
        s.setsockopt(zmq.HWM, self.zmq_hwm)
        if self.zmq_rcvbuf > 0:
            s.setsockopt (zmq.RCVBUF, self.zmq_rcvbuf)
        if self.zmq_sndbuf > 0:
            s.setsockopt (zmq.SNDBUF, self.zmq_sndbuf)
        if self.zmq_tcp_keepalive > 0:
            if getattr(zmq, 'TCP_KEEPALIVE', -1) > 0:
                s.setsockopt(zmq.TCP_KEEPALIVE, self.zmq_tcp_keepalive)
                s.setsockopt(zmq.TCP_KEEPALIVE_INTVL, self.zmq_tcp_keepalive_intvl)
                s.setsockopt(zmq.TCP_KEEPALIVE_IDLE, self.zmq_tcp_keepalive_idle)
                s.setsockopt(zmq.TCP_KEEPALIVE_CNT, self.zmq_tcp_keepalive_cnt)
            else:
                self.log.info("TCP_KEEPALIVE not available")
        s.bind(self.local_url)
        self.local = CCStream(s, self.ioloop, qmaxsize = self.zmq_hwm)
        self.local.on_recv(self.handle_cc_recv)

        self.handlers = {}
        self.routes = {}
        rcf = skytools.Config('routes', self.cf.filename, ignore_defs = True)
        for r, hnames in rcf.cf.items('routes'):
            self.log.info ('New route: %s = %s', r, hnames)
            for hname in [hn.strip() for hn in hnames.split(',')]:
                if hname in self.handlers:
                    h = self.handlers[hname]
                else:
                    hcf = self.cf.clone(hname)

                    # renamed option: plugin->handler
                    htype = hcf.get('plugin', '?')
                    if htype == '?':
                        htype = hcf.get('handler')

                    cls = cc_handler_lookup(htype, self.cur_role)
                    h = cls(hname, hcf, self)
                    self.handlers[hname] = h
                self.add_handler(r, h)

        self.stimer = PeriodicCallback(self.send_stats, 30*1000, self.ioloop)
        self.stimer.start()

    def send_stats(self):
        if self.stat_level == 0:
            return

        # make sure we have something to send
        self.stat_increase('count', 0)

        # combine our stats with global stats
        self.combine_stats (reset_stats())

        super(CCServer, self).send_stats()

    def combine_stats (self, other):
        for k,v in other.items():
            self.stat_increase(k,v)

    def add_handler(self, rname, handler):
        """Add route to handler"""

        if rname == '*':
            r = ()
        else:
            r = tuple(rname.split('.'))
        self.log.debug('New route for handler: %r -> %s', r, handler.hname)
        rhandlers = self.routes.setdefault(r, [])
        rhandlers.append(handler)

    def handle_cc_recv(self, zmsg):
        """Got message from client, pick handler."""

        start = time.time()
        self.stat_inc ('count')
        self.log.trace('got msg: %r', zmsg)
        try:
            cmsg = CCMessage(zmsg)
        except:
            self.log.exception('Invalid CC message')
            self.stat_increase('count.invalid')
            return

        try:
            dst = cmsg.get_dest()
            size = cmsg.get_size()
            route = tuple(dst.split('.'))

            # find and run all handlers that match
            cnt = 0
            for n in range(0, 1 + len(route)):
                p = route[ : n]
                for h in self.routes.get(p, []):
                    self.log.trace('calling handler %s', h.hname)
                    h.handle_msg(cmsg)
                    cnt += 1
            if cnt == 0:
                self.log.warning('dropping msg, no route: %s', dst)
                stat = 'dropped'
            else:
                stat = 'ok'

        except Exception:
            self.log.exception('crashed, dropping msg: %s', dst)
            stat = 'crashed'

        # update stats
        taken = time.time() - start
        self.stat_inc ('bytes', size)
        self.stat_inc ('seconds', taken)
        self.stat_inc ('count.%s' % stat)
        self.stat_inc ('bytes.%s' % stat, size)
        self.stat_inc ('seconds.%s' % stat, taken)
        if self.stat_level > 1:
            self.stat_inc ('count.%s.msg.%s' % (stat, dst))
            self.stat_inc ('bytes.%s.msg.%s' % (stat, dst), size)
            self.stat_inc ('seconds.%s.msg.%s' % (stat, dst), taken)

    def work(self):
        """Default work loop simply runs ioloop."""
        self.set_single_loop(1)
        self.log.info('Starting IOLoop')
        try:
            self.ioloop.start()
        except zmq.ZMQError, d:
            # ZMQ gets surprised by EINTR
            if d.errno == errno.EINTR:
                return 1
            raise

    def run (self):
        """ Thread main loop. """
        super(CCServer, self).run()
        #ver = map(int, skytools.__version__.split('.'))
        from skytools.natsort import natsort_key
        ver = natsort_key (skytools.__version__)
        if ver <= [3, '.', 1]:
            self.shutdown()

    def stop(self):
        """Called from signal handler"""
        super(CCServer, self).stop()
        self.ioloop.stop()

    def shutdown (self):
        """ Called just after exiting main loop. """
        self.log.info("Stopping CC handlers")
        for h in self.handlers.values():
            self.log.debug("stopping %s", h.hname)
            h.stop()


def main():
    script = CCServer('ccserver', sys.argv[1:])
    script.start()

if __name__ == '__main__':
    main()
