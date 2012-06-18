"""CC handler classes.

Called in CC main loop, from single thread.
They need to do something with the message and *fast*:

- Push into remote ZMQ socket
- Push into local ZMQ socket to local worker processes/threads
- Write into file? (best done async / in worker)

- No time-consuming processing.

It would be preferable to reduce everything to write to socket.
"""

import sys

import skytools

__all__ = ['CCHandler', 'cc_handler_lookup']

#
# Base class for handlers
#

class CCHandler(object):
    """Basic handler interface."""

    log = skytools.getLogger('h:CCHandler')

    def __init__(self, hname, hcf, ccscript):
        """Store handler config."""
        self.hname = hname
        self.cf = hcf
        self.xtx = ccscript.xtx
        self.zctx = ccscript.zctx
        self.ioloop = ccscript.ioloop
        self.cclocal = ccscript.local
        self.stat_inc = ccscript.stat_increase

    def handle_msg(self, rmsg):
        """Process single message"""
        raise NotImplementedError

    def stop(self):
        """Called on process shutdown."""
        pass

#
# Handler lookup
#

_short_names = {
    'dbhandler': 'cc.handler.database',
    'disposer': 'cc.handler.disposer',
    'echo': 'cc.handler.echo',
    'infowriter': 'cc.handler.infowriter',
    'jobmgr': 'cc.handler.jobmgr',
    'locallogger': 'cc.handler.locallogger',
    'proxy': 'cc.handler.proxy',
    'tailwriter': 'cc.handler.tailwriter',
    'taskrouter': 'cc.handler.taskrouter',
}

def cc_handler_lookup(name, cur_role):
    name = _short_names.get(name, name)
    __import__(name)
    m = sys.modules[name]
    cls = getattr(m, m.CC_HANDLER)
    if cur_role == 'insecure':
        return cls
    if cur_role not in cls.CC_ROLES:
        raise Exception('Handler %s cannot be run in role %s' % (name, cur_role))
    return cls
