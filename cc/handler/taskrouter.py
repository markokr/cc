
import time

from zmq.eventloop.ioloop import PeriodicCallback
from cc.handler import CCHandler

import skytools

__all__ = ['TaskRouter']

CC_HANDLER = 'TaskRouter'

#
# task router
#

class HostRoute(object):
    """ZMQ route for one host."""

    __slots__ = ('host', 'route', 'create_time')

    def __init__(self, host, route):
        assert isinstance(route, list)
        self.host = host
        self.route = route
        self.create_time = time.time()

class TaskRouter(CCHandler):
    """Keep track of host routes.
    
    Clean old ones.
    """

    CC_ROLES = ['remote']

    def __init__(self, *args):
        super(TaskRouter, self).__init__(*args)
        self.route_map = {}
        
        # 1 hr?
        self.route_lifetime = 1 * 60 * 60
        self.maint_period = 1 * 60

        self.timer = PeriodicCallback(self.do_maint, self.maint_period*1000, self.ioloop)
        self.timer.start()

    def handle_msg(self, cmsg):

        msg = cmsg.get_payload(self.xtx)
        req = cmsg.get_dest()
        route = cmsg.get_route()

        cmd = msg.req
        host = msg.host

        if req == 'task.register':
            self.register_host(host, route)
        elif req == 'task.send':
            self.send_host(host, cmsg)
        else:
            self.log.warning('TaskRouter: unknown msg: %s', req)

    def do_maint(self):
        """Drop old routes"""
        self.log.info('TaskRouter.do_maint')
        now = time.time()
        zombies = []
        for hr in self.route_map.itervalues():
            if now - hr.create_time > self.route_lifetime:
                zombies.append(hr)
        for hr in zombies:
            self.log.info('TaskRouter: deleting route for %s', hr.host)
            del self.route_map[hr.host]

    def send_host(self, host, cmsg):
        """Send message for task executor on host"""

        if host not in self.route_map:
            self.log.info('TaskRouter: cannot route to %s', host)
            return

        # find ZMQ route
        hr = self.route_map[host]

        # re-construct message
        msg = cmsg.get_non_route()
        zmsg = hr.route + [''] + msg

        # send the message
        self.log.info('TaskRouter: sending task to %s', host)
        self.cclocal.send_multipart(zmsg)

        # FIXME: proper reply?
        zans = cmsg.get_route() + [''] + ['OK']
        self.cclocal.send_multipart(zans)

    def register_host(self, host, route):
        """Remember ZMQ route for host"""
        self.log.info('register_host(%s, %s)', repr(host), repr(route))
        hr = HostRoute(host, route)
        self.route_map[hr.host] = hr

        # FIXME: proper reply?
        #zans = route + [''] + ['OK']
        #self.cclocal.send_multipart(zans)

