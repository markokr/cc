"""Local logger."""

from cc.handler import CCHandler

__all__ = ['LocalLogger']

CC_HANDLER = 'LocalLogger'

class LocalLogger(CCHandler):
    """Log as local log msg."""

    CC_ROLES = ['local', 'remote']

    def handle_msg(self, cmsg):
        msg = cmsg.get_payload(self.xtx)
        self.log.info ('[%s@%s] %s %s', msg.job_name, msg.hostname, msg.log_level, msg.log_msg)
