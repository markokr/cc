"""CC handler classes.

Called in CC main loop, from single thread.
They need to do something with the message and *fast*:

- Push into remote ZMQ socket
- Push into local ZMQ socket to local worker processes/threads
- Write into file?

- No time-consuming processing.

It would be preferable to reduce everything to write to socket.

"""

import sys

__all__ = ['CCHandler', 'cc_handler_lookup']

#
# Base class for handlers
#

class CCHandler(object):
    """Basic handler interface."""

    def __init__(self, hname, hcf, ccscript):
        """Store handler config."""
        self.hname = hname
        self.cf = hcf
        self.cclocal = ccscript.local
        self.zctx = ccscript.zctx
        self.ioloop = ccscript.ioloop
        self.log = ccscript.log

    def handle_msg(self, rmsg):
        """Process single message"""
        pass

    def stop(self):
        """Called on process shutdown."""
        pass

#
# Handler lookup
#

_short_names = {
    'proxy': 'cc.handler.proxy',
    'dbhandler': 'cc.handler.database',
    'taskrouter': 'cc.handler.taskrouter',
    'infowriter': 'cc.handler.infowriter',
    'jobmgr': 'cc.handler.jobmgr',
    'locallogger': 'cc.handler.locallogger',
}

def cc_handler_lookup(name):
    name = _short_names.get(name, name)
    __import__(name)
    m = sys.modules[name]
    cname = getattr(m, 'CC_HANDLER')
    return getattr(m, cname)

