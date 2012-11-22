"""
Filters received messages, then dispatches them to another handler.
"""

import fnmatch
import re

import skytools

from cc.handler import CCHandler

__all__ = ['Filter']

CC_HANDLER = 'Filter'

class Filter (CCHandler):
    """ Filters received messages, then dispatches them to another handler. """

    CC_ROLES = ['local', 'remote']

    log = skytools.getLogger ('h:Filter')

    def __init__ (self, hname, hcf, ccscript):
        super(Filter, self).__init__(hname, hcf, ccscript)

        self.fwd_hname = self.cf.get ('forward-to')
        self.fwd_handler = ccscript.get_handler (self.fwd_hname)

        self.includes = _hint_list (self.cf.getlist ('include', []))
        self.excludes = _hint_list (self.cf.getlist ('exclude', []))

    def handle_msg (self, cmsg):
        """ Got message from client -- process it.
        """
        dest = cmsg.get_dest()
        size = cmsg.get_size()
        stat = '?'

        for exc, wild in self.excludes:
            if (not wild and dest == exc) or fnmatch.fnmatchcase (dest, exc):
                stat = 'dropped'
                break
        else:
            if self.includes:
                for inc, wild in self.includes:
                    if (not wild and dest == inc) or fnmatch.fnmatchcase (dest, inc):
                        break
                else:
                    stat = 'dropped'
            if stat != 'dropped':
                try:
                    self.fwd_handler.handle_msg (cmsg)
                    stat = 'ok'
                except Exception:
                    self.log.exception ('crashed, dropping msg: %s', dest)
                    stat = 'crashed'

        self.stat_inc ('filter.count')
        self.stat_inc ('filter.bytes', size)
        self.stat_inc ('filter.count.%s' % stat)
        self.stat_inc ('filter.bytes.%s' % stat, size)


def _hint_list (ilist):
    olist = []
    for item in ilist:
        wild = re.search ('\?|\*', item) is not None
        olist.append ((item, wild))
    return olist
