"""Wrapper around ZMQStream
"""

from zmq.eventloop.zmqstream import ZMQStream
from cc.message import CCMessage

__all__ = ['CCStream']

class CCStream(ZMQStream):
    """Add CCMessage methods to ZMQStream"""

    def send_cmsg(self, cmsg):
        """Send CCMessage to socket"""
        self.send_multipart(cmsg.zmsg)

    def on_recv_cmsg(self, cbfunc):
        """Set callback that receives CCMessage."""
        def convert_cmsg(zmsg):
            cmsg = CCMessage(zmsg)
            cbfunc(cmsg)
        self.on_recv(convert_cmsg)

