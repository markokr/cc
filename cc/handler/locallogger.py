"""Local logger."""

import logging
import time

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
            lt = time.strftime ("%H:%M:%S,", time.localtime (msg.log_time)) + ("%.3f" % (msg.log_time % 1))[2:]
            self.log.info ('[%s@%s] %s %s %s', msg.job_name, msg.hostname, lt, msg.log_level, msg.log_msg)
        else:
            self.log.info ('non-log msg: %r', msg)
