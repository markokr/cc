import skytools

from cc.handler import CCHandler

__all__ = ['Disposer']

CC_HANDLER = 'Disposer'

class Disposer (CCHandler):
    """ Discards any message received """

    CC_ROLES = ['local', 'remote']

    log = skytools.getLogger ('h:Disposer')

    def handle_msg (self, cmsg):
        """ Got message from client -- discard it :-) """
        self.log.trace('')
        self.stat_inc ('disposed_count')
        self.stat_inc ('disposed_bytes', cmsg.get_size())
