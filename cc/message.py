
__all__ = ['CCMessage', 'assert_msg_req', 'is_msg_req_valid', 'zmsg_size']

import re

MSG_DST_VALID = re.compile (r'^[a-zA-Z0-9_-]+(?:[.][a-zA-Z0-9_-]+)*$')


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
    - cc blob data
    """
    __slots__ = ('zmsg', 'rpos', 'parsed', 'signature')

    def __init__(self, zmsg):
        assert isinstance(zmsg, list)
        self.zmsg = zmsg
        self.rpos = zmsg.index('')
        self.parsed = None
        self.signature = None
        assert_msg_req (self.get_dest())

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
        """Return signature"""
        if self.rpos + 3 >= len(self.zmsg):
            return ''
        return self.zmsg[self.rpos + 3]

    def get_part3(self):
        """Return blob"""
        if self.rpos + 4 >= len(self.zmsg):
            return None
        return self.zmsg[self.rpos + 4]

    def get_size(self):
        return zmsg_size (self.zmsg)

    def __repr__(self):
        x = repr(self.zmsg)
        if len(x) > 300:
            x = x[:300] + '...'
        return 'CCMessage(%s)' % x

    def __str__(self):
        x = repr(self.get_non_route())
        if len(x) > 300:
            x = x[:300] + '...'
        return 'CCMessage(%s)' % x

    def set_route (self, route):
        """Fill local route with another route."""
        self.zmsg[:self.rpos] = route
        self.rpos = len(route)

    def take_route(self, cmsg):
        """Fill local route with route from another message."""
        r = cmsg.get_route()
        self.set_route(r)

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

    def send_to (self, sock):
        sock.send_multipart (self.zmsg)


def assert_msg_req (dest):
    assert MSG_DST_VALID.match (dest) , "invalid msg dest: %r" % dest

def is_msg_req_valid (dest):
    return (MSG_DST_VALID.match (dest) is not None)

def zmsg_size (zmsg):
    n = 0
    for p in zmsg:
        n += len(p)
    return n
