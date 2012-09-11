"""
Echo handler / sender / monitor.

. Passive (implicit) echo responding.
. Active (explicit) echo requesting and monitoring.
"""

import time

import skytools
import zmq
from zmq.eventloop.ioloop import PeriodicCallback

from cc.handler import CCHandler
from cc.message import CCMessage
from cc.reqs import EchoRequestMessage, EchoResponseMessage
from cc.stream import CCStream

__all__ = ['Echo']

CC_HANDLER = 'Echo'

class Echo (CCHandler):
    """ Echo handler / sender / monitor """

    CC_ROLES = ['local', 'remote']

    log = skytools.getLogger ('h:Echo')

    ping_tick = 1
    zmq_hwm = 1
    zmq_linger = 0

    def __init__ (self, hname, hcf, ccscript):
        super(Echo, self).__init__(hname, hcf, ccscript)

        self.echoes = {} # echo stats for monitored peers
        self.stream = {} # connections to monitored peers

        for url in self.cf.getlist ("ping-remotes", ""):
            sock = self._make_socket (url)
            self.stream[url] = CCStream (sock, ccscript.ioloop, qmaxsize = self.zmq_hwm)
            self.stream[url].on_recv (self.on_recv)
            self.echoes[url] = EchoState (url)
            self.log.debug ("will ping %s", url)

        self.timer = PeriodicCallback (self.ping, self.ping_tick * 1000, self.ioloop)
        self.timer.start()

    def _make_socket (self, url):
        """ Create socket for pinging remote CC. """
        sock = self.zctx.socket (zmq.XREQ)
        sock.setsockopt (zmq.HWM, self.zmq_hwm)
        sock.setsockopt (zmq.LINGER, self.zmq_linger)
        sock.connect (url)
        return sock

    def on_recv (self, zmsg):
        """ Got reply from a remote CC, process it. """
        try:
            self.log.trace ("%r", zmsg)
            cmsg = CCMessage (zmsg)
            req = cmsg.get_dest()
            if req == "echo.response":
                self.process_response (cmsg)
            else:
                self.log.warn ("unknown msg: %s", req)
        except:
            self.log.exception ("crashed, dropping msg")

    def handle_msg (self, cmsg):
        """ Got a message, process it. """

        self.log.trace ("%r", cmsg)
        req = cmsg.get_dest()

        if req == "echo.request":
            self.process_request (cmsg)
        else:
            self.log.warn ("unknown msg: %s", req)

    def process_request (self, cmsg):
        """ Ping received, respond with pong. """

        msg = cmsg.get_payload (self.xtx)
        if not msg: return

        rep = EchoResponseMessage(
                orig_hostname = msg['hostname'],
                orig_target = msg['target'],
                orig_time = msg['time'])
        rcm = self.xtx.create_cmsg (rep)
        rcm.take_route (cmsg)
        rcm.send_to (self.cclocal)

    def process_response (self, cmsg):
        """ Pong received, evaluate it. """

        msg = cmsg.get_payload (self.xtx)
        if not msg: return

        url = msg.orig_target
        if url not in self.echoes:
            self.log.warn ("unknown pong: %s", url)
            return
        echo = self.echoes[url]
        echo.update_pong (msg)

        rtt = echo.time_pong - msg.orig_time
        if msg.orig_time == echo.time_ping:
            self.log.trace ("echo time: %f s (%s)", rtt, url)
        elif rtt <= 5 * self.ping_tick:
            self.log.debug ("late pong: %f s (%s)", rtt, url)
        else:
            self.log.info ("too late pong: %f s (%s)", rtt, url)

    def send_request (self, url):
        """ Send ping to remote CC. """
        msg = EchoRequestMessage(
                target = url)
        cmsg = self.xtx.create_cmsg (msg)
        self.stream[url].send_cmsg (cmsg)
        self.echoes[url].update_ping (msg)
        self.log.trace ("%r", msg)

    def ping (self):
        """ Echo requesting and monitoring. """
        self.log.trace ("")
        for url in self.stream:
            echo = self.echoes[url]
            if echo.time_ping - echo.time_pong > 5 * self.ping_tick:
                self.log.warn ("no pong from %s for %f s", url, echo.time_ping - echo.time_pong)
            self.send_request (url)

    def stop (self):
        super(Echo, self).stop()
        self.log.info ("stopping")
        self.timer.stop()


class EchoState (object):
    def __init__ (self, url):
        now = time.time()
        self.target = url
        self.time_ping = now
        self.time_pong = now
        self.count_ping = 0
        self.count_pong = 0

    def update_ping (self, msg):
        assert self.target == msg.target
        self.time_ping = msg.time
        self.count_ping += 1

    def update_pong (self, msg):
        assert self.target == msg.orig_target
        self.time_pong = time.time()
        self.count_pong += 1
