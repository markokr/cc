"""
Delays all received messages, then dispatches them to another handler.
"""

import collections
import time

import skytools
from zmq.eventloop.ioloop import PeriodicCallback

from cc.handler import CCHandler

__all__ = ['Delay']

CC_HANDLER = 'Delay'

class Delay (CCHandler):
    """ Delays all received messages, then dispatches them to another handler. """

    CC_ROLES = ['local', 'remote']

    log = skytools.getLogger ('h:Delay')

    tick = 250 # ms

    def __init__ (self, hname, hcf, ccscript):
        super(Delay, self).__init__(hname, hcf, ccscript)

        self.fwd_hname = self.cf.get ('forward-to')
        self.delay = self.cf.getint ('delay', 0)

        self.fwd_handler = ccscript.get_handler (self.fwd_hname)
        self.queue = collections.deque()

        self.timer = PeriodicCallback (self.process_queue, self.tick, self.ioloop)
        self.timer.start()

    def handle_msg (self, cmsg):
        """ Got message from client -- queue it """
        self.queue.append ((time.time() + self.delay, cmsg))

    def process_queue (self):
        now = time.time()
        try:
            while (self.queue[0][0] <= now):
                at, cmsg = self.queue.popleft()
                size = cmsg.get_size()
                try:
                    self.fwd_handler.handle_msg (cmsg)
                    stat = 'ok'
                except Exception:
                    self.log.exception ('crashed, dropping msg: %s', cmsg.get_dest())
                    stat = 'crashed'
                self.stat_inc ('delay.count')
                self.stat_inc ('delay.bytes', size)
                self.stat_inc ('delay.count.%s' % stat)
                self.stat_inc ('delay.bytes.%s' % stat, size)
        except IndexError:
            pass
