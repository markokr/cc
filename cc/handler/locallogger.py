
from cc.handler import CCHandler

__all__ = ['LocalLogger']
CC_HANDLER = 'LocalLogger'

#
# local logger
#

class LocalLogger(CCHandler):
    """Log as local log msg."""
    def handle_msg(self, cmsg):
        data = cmsg.get_payload()
        self.log.info('[%s] %s %s', data['job_name'], data['level'], data['msg'])


