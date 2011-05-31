
import json

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
    __slots__ = ('zmsg', 'rpos', 'parsed')

    def __init__(self, zmsg):
        assert isinstance(zmsg, list)
        self.zmsg = zmsg
        self.rpos = zmsg.index('')
        self.parsed = None

    def get_route(self):
        """Route parts"""
        return self.zmsg[ : self.rpos]

    def get_non_route(self):
        """Payload parts"""
        return self.zmsg[ self.rpos + 1 : ]

    def get_dest(self):
        """Return destination part"""
        return self.zmsg[self.rpos + 1]

    def get_payload_json(self):
        """Return body (json) as string"""
        return self.zmsg[self.rpos + 2]

    def get_sig(self):
        """Retrun signature"""
        return self.zmsg[self.rpos + 3]

    def get_payload(self):
        """Get parsed payload"""
        if self.parsed is None:
            js = self.get_payload_json()
            self.parsed = json.loads(js)
        return self.parsed

    def get_size(self):
        n = 0
        for p in self.zmsg:
            n += len(p)
        return n
