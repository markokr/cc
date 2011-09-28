#! /usr/bin/env python

"""Command-and-Control server.

client <-> ccserver|handler <-> handlerproc


"""


import sys, errno, os.path
import zmq, zmq.eventloop

import skytools

from zmq.eventloop.ioloop import PeriodicCallback

from cc.message import CCMessage
from cc.stream import CCStream
from cc.handler import cc_handler_lookup
from cc.crypto import CryptoContext


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

    def print_ini(self):
        super(CCServer, self).print_ini()

        self._print_ini_frag(self.extra_ini)

    def startup(self):
        """Setup sockets and handlers."""

        super(CCServer, self).startup()

        self.xtx = CryptoContext(self.cf, self.log)
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
            self.log.info ('CCServer.startup: Route def: %s = %s', r, hnames)
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

        self.stimer = PeriodicCallback(self.send_stats, 5*1000, self.ioloop)
        self.stimer.start()

    def add_handler(self, rname, handler):
        """Add route to handler"""

        r = tuple(rname.split('.'))
        self.log.info('CCServer.add_handler: %s -> %s', repr(r), handler.hname)
        rhandlers = self.routes.setdefault(r, [])
        rhandlers.append(handler)

    def handle_cc_recv(self, zmsg):
        """Got message from client, pick handler."""

        self.log.debug('CCServer.handle_cc_recv: %r', zmsg)
        try:
            cmsg = CCMessage(zmsg)
        except:
            self.log.exception('CCServer.handle_cc_recv: Invalid ZMQ format')
            return

        try:
            dst = cmsg.get_dest()
            route = tuple(dst.split('.'))

            # find and run all handlers that match
            cnt = 0
            for n in range(1, 1 + len(route)):
                p = route[ : n]
                for h in self.routes.get(p, []):
                    self.log.debug('CCServer.handle_cc_recv: calling handler %s', h.hname)
                    h.handle_msg(cmsg)
                    cnt += 1
            if cnt == 0:
                self.log.warning('CCServer.handle_cc_recv: dropping msg, no route: %s', dst)

            # update stats
            self.stat_increase('count')
            self.stat_increase('bytes', cmsg.get_size())

        except Exception:
            self.log.exception('CCServer.handle_cc_recv crashed, dropping msg: %s', cmsg.get_dest())

    def work(self):
        """Default work loop simply runs ioloop."""
        self.set_single_loop(1)
        self.log.info('CCServer.work: Starting IOLoop')
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
        for h in self.handlers.values():
            h.stop()

def main():
    script = CCServer('ccserver', sys.argv[1:])
    script.start()

if __name__ == '__main__':
    main()

