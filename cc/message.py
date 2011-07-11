
from cc import json

__all__ = ['CCMessage']

class CCMessage(object):
    """CC multipart message.
    
    Format is similar to usual REQ+REP sockets:

    - id_hopX
    - id_hop2
    - id_hop1
    - id_req (optional)
    - empty part
    - cc dest
    - cc payload (json)
    - cc signature
    """
    __slots__ = ('zmsg', 'rpos', 'parsed', 'signature')

    def __init__(self, zmsg):
        assert isinstance(zmsg, list)
        self.zmsg = zmsg
        self.rpos = zmsg.index('')
        self.parsed = None
        self.signature = None

    def get_route(self):
        """Route parts"""
        return self.zmsg[ : self.rpos]

    def get_non_route(self):
        """Payload parts"""
        return self.zmsg[ self.rpos + 1 : ]

    def get_dest(self):
        """Return destination part"""
        return self.zmsg[self.rpos + 1]

    def get_part1(self):
        """Return body (json) as string"""
        return self.zmsg[self.rpos + 2]

    def get_part2(self):
        """Retrun signature"""
        if self.rpos + 3 >= len(self.zmsg):
            return ''
        return self.zmsg[self.rpos + 3]

    def get_size(self):
        n = 0
        for p in self.zmsg:
            n += len(p)
        return n

    def __str__(self):
        x = repr(self.zmsg)
        if len(x) > 200:
            x = x[:200] + ' ...'
        return 'CCMessage%s' % x

    def __repr__(self):
        return self.__str__()

    def take_route(self, cmsg):
        """Fill local route with route from another message."""
        r = cmsg.get_route()
        self.zmsg[:self.rpos] = r
        self.rpos = len(r)

    def get_payload(self, xtx):
        if self.parsed:
            return self.parsed
        msg, sgn = xtx.parse_cmsg(self)
        if not msg:
            return None
        self.parsed = msg
        self.signature = sgn
        return msg

    def get_signature(self, xtx):
        self.get_payload(xtx)
        return self.signature

