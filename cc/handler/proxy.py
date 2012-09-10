"""
Proxy -- forwards messages to/from remote CC.
"""

import skytools
import zmq
from zmq.eventloop.ioloop import PeriodicCallback

from cc.handler import CCHandler
from cc.handler.echo import EchoState
from cc.message import CCMessage, zmsg_size
from cc.reqs import EchoRequestMessage
from cc.stream import CCStream
from cc.util import hsize_to_bytes

__all__ = ['ProxyHandler', 'BaseProxyHandler']

CC_HANDLER = 'ProxyHandler'

#
# base message proxy class
#

class BaseProxyHandler (CCHandler):
    """Simply proxies further"""

    CC_ROLES = ['local', 'remote']

    log = skytools.getLogger ('h:BaseProxyHandler')

    zmq_hwm = 100
    zmq_linger = 500
    zmq_rcvbuf = 0 # means no change
    zmq_sndbuf = 0 # means no change

    zmq_tcp_keepalive = 1
    zmq_tcp_keepalive_intvl = 15
    zmq_tcp_keepalive_idle = 4*60
    zmq_tcp_keepalive_cnt = 4 # 9 on win32

    def __init__(self, hname, hcf, ccscript):
        super(BaseProxyHandler, self).__init__(hname, hcf, ccscript)

        s = self.make_socket()
        self.stream = CCStream(s, ccscript.ioloop)
        self.stream.on_recv(self.on_recv)

        self.startup()
        self.launch_workers()

    def startup(self):
        pass

    def launch_workers(self):
        pass

    def make_socket(self):
        self.zmq_hwm = self.cf.getint ('zmq_hwm', self.zmq_hwm)
        self.zmq_linger = self.cf.getint ('zmq_linger', self.zmq_linger)
        self.zmq_rcvbuf = hsize_to_bytes (self.cf.get ('zmq_rcvbuf', str(self.zmq_rcvbuf)))
        self.zmq_sndbuf = hsize_to_bytes (self.cf.get ('zmq_sndbuf', str(self.zmq_sndbuf)))
        self.zmq_tcp_keepalive = self.cf.getint ('zmq_tcp_keepalive', self.zmq_tcp_keepalive)
        self.zmq_tcp_keepalive_intvl = self.cf.getint ('zmq_tcp_keepalive_intvl', self.zmq_tcp_keepalive_intvl)
        self.zmq_tcp_keepalive_idle = self.cf.getint ('zmq_tcp_keepalive_idle', self.zmq_tcp_keepalive_idle)
        self.zmq_tcp_keepalive_cnt = self.cf.getint ('zmq_tcp_keepalive_cnt', self.zmq_tcp_keepalive_cnt)
        self.remote_url = self.cf.get ('remote-cc')
        s = self.zctx.socket(zmq.XREQ)
        s.setsockopt (zmq.HWM, self.zmq_hwm)
        s.setsockopt (zmq.LINGER, self.zmq_linger)
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
        s.connect (self.remote_url)
        return s

    def on_recv(self, zmsg):
        """Got message from remote CC, send to client."""
        try:
            self.log.trace('')
            self.stat_inc('count')
            self.stat_inc('bytes', zmsg_size(zmsg))
            self.cclocal.send_multipart(zmsg)
        except:
            self.log.exception('crashed, dropping msg')

    def handle_msg(self, cmsg):
        """Got message from client, send to remote CC."""
        self.log.trace('')
        self.stream.send_cmsg(cmsg)

#
# full featured message proxy
#

class ProxyHandler (BaseProxyHandler):
    """ Simply proxies further """

    log = skytools.getLogger ('h:ProxyHandler')

    ping_tick = 1

    def __init__ (self, hname, hcf, ccscript):
        super(ProxyHandler, self).__init__(hname, hcf, ccscript)

        self.ping_remote = self.cf.getbool ("ping", False)
        if self.ping_remote:
            self.echo_stats = EchoState (self.remote_url)
            self.echo_timer = PeriodicCallback (self.ping, self.ping_tick * 1000, self.ioloop)
            self.echo_timer.start()
            self.log.debug ("will ping %s", self.remote_url)

    def on_recv (self, zmsg):
        """ Got message from remote CC, process it. """
        try:
            # pongs to our pings should come back w/o any routing info
            if self.ping_remote and zmsg[0] == '':
                self.log.trace ("%r", zmsg)
                cmsg = CCMessage (zmsg)
                req = cmsg.get_dest()
                if req == "echo.response":
                    self._recv_pong (cmsg)
                else:
                    self.log.warn ("unknown msg: %s", req)
        except:
            self.log.exception ("crashed")
        finally:
            super(ProxyHandler, self).on_recv(zmsg)

    def _recv_pong (self, cmsg):
        """ Pong received, evaluate it. """

        msg = cmsg.get_payload (self.xtx)
        if not msg: return

        if msg.orig_target != self.remote_url:
            self.log.warn ("unknown pong: %s", msg.orig_target)
            return
        echo = self.echo_stats
        echo.update_pong (msg)

        rtt = echo.time_pong - msg.orig_time
        if msg.orig_time == echo.time_ping:
            self.log.trace ("echo time: %f s (%s)", rtt, self.remote_url)
        elif rtt <= 5 * self.ping_tick:
            self.log.debug ("late pong: %f s (%s)", rtt, self.remote_url)
        else:
            self.log.info ("too late pong: %f s (%s)", rtt, self.remote_url)

    def _send_ping (self):
        """ Send ping to remote CC. """
        msg = EchoRequestMessage(
                target = self.remote_url)
        cmsg = self.xtx.create_cmsg (msg)
        self.stream.send_cmsg (cmsg)
        self.echo_stats.update_ping (msg)
        self.log.trace ("%r", msg)

    def ping (self):
        """ Echo requesting and monitoring. """
        self.log.trace ("")
        echo = self.echo_stats
        if echo.time_ping - echo.time_pong > 5 * self.ping_tick:
            self.log.warn ("no pong from %s for %f s", self.remote_url, echo.time_ping - echo.time_pong)
        self._send_ping ()

    def stop (self):
        super(ProxyHandler, self).stop()
        self.log.info ("stopping")
        if hasattr (self, "echo_timer"):
            self.echo_timer.stop()
