#! /usr/bin/env python

from zmq.eventloop.zmqstream import ZMQStream
from cc.message import CCMessage

__all__ = ['CCStream']

class CCStream(ZMQStream):
    """Add CCMessage methods to ZMQStream"""

    def send_cmsg(self, cmsg):
        """Send CCMessage to socket"""
        self.send_multipart(cmsg.zmsg)

    def recv_cmsg(self):
        """Read CCMessage from socket"""
        zmsg = self.recv_multipart()
        return CCMessage(zmsg)

    def on_recv_cmsg(self, cbfunc):
        def convert_cmsg(zmsg):
            cmsg = CCMessage(zmsg)
            cbfunc(cmsg)
        self.on_recv(convert_cmsg)

