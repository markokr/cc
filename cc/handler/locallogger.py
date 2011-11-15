"""Local logger."""

import logging

from cc.handler import CCHandler

__all__ = ['LocalLogger']

CC_HANDLER = 'LocalLogger'

class LocalLogger(CCHandler):
    """Log as local log msg."""

    CC_ROLES = ['local', 'remote']

    log = logging.getLogger('h:LocalLogger')

    def handle_msg(self, cmsg):
        msg = cmsg.get_payload(self.xtx)
        if hasattr(msg, 'log_level'):
            self.log.info ('[%s@%s] %s %s', msg.job_name, msg.hostname, msg.log_level, msg.log_msg)
        else:
            self.log.info ('non-log msg: %r', msg)
