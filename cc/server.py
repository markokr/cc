#! /usr/bin/env python

"""Command-and-Control server.

client <-> ccserver|handler <-> handlerproc


"""


import errno
import logging
import os.path
import sys

import skytools
import zmq, zmq.eventloop
from zmq.eventloop.ioloop import PeriodicCallback

from cc.message import CCMessage
from cc.stream import CCStream
from cc.handler import cc_handler_lookup
from cc.crypto import CryptoContext


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
        cc-socket = tcp://127.0.0.1:10000
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

    log = logging.getLogger('CCServer')

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

    def print_ini(self):
        super(CCServer, self).print_ini()

        self._print_ini_frag(self.extra_ini)

    def startup(self):
        """Setup sockets and handlers."""

        super(CCServer, self).startup()

        self.xtx = CryptoContext(self.cf)
        self.zctx = zmq.Context()
        self.ioloop = zmq.eventloop.IOLoop.instance()

        self.local_url = self.cf.get('cc-socket')

        self.cur_role = self.cf.get('cc-role', 'insecure')
        if self.cur_role == 'insecure':
            self.log.warning('CC is running in insecure mode, please add "cc-role = local" or "cc-role = remote" option to config')

        # initialize local listen socket
        s = self.zctx.socket(zmq.XREP)
        s.bind(self.local_url)
        s.setsockopt(zmq.LINGER, 500)
        self.local = CCStream(s, self.ioloop)
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
        # make sure we have something to send
        self.stat_increase('count', 0)

        super(CCServer, self).send_stats()

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

        self.log.trace('got msg: %r', zmsg)
        try:
            cmsg = CCMessage(zmsg)
        except:
            self.log.exception('Invalid CC message')
            return

        try:
            dst = cmsg.get_dest()
            route = tuple(dst.split('.'))

            # find and run all handlers that match
            cnt = 0
            for n in range(0, 1 + len(route)):
                p = route[ : n]
                for h in self.routes.get(p, []):
                    self.log.debug('calling handler %s', h.hname)
                    h.handle_msg(cmsg)
                    cnt += 1
            if cnt == 0:
                self.log.warning('dropping msg, no route: %s', dst)

            # update stats
            self.stat_increase('count')
            self.stat_increase('bytes', cmsg.get_size())

        except Exception:
            self.log.exception('crashed, dropping msg: %s', cmsg.get_dest())

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

    def stop(self):
        """Called from signal handler"""
        super(CCServer, self).stop()
        self.ioloop.stop()

        # FIXME: this should be done outside signal handler
        self.log.info("Stopping CC handlers")
        for h in self.handlers.values():
            h.stop()


def main():
    script = CCServer('ccserver', sys.argv[1:])
    script.start()

if __name__ == '__main__':
    main()
