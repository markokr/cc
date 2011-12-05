
import time

from zmq.eventloop.ioloop import PeriodicCallback

from cc.handler import CCHandler
from cc.reqs import TaskReplyMessage, ErrorMessage

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

class ReplyRoute(object):
    """ZMQ route for reply to task."""

    __slots__ = ('uid', 'route', 'atime')

    def __init__(self, uid, route):
        assert isinstance(route, list)
        self.uid = uid
        self.route = route
        self.atime = time.time()


class TaskRouter(CCHandler):
    """Keep track of host routes.

    Clean old ones.
    """

    log = skytools.getLogger('h:TaskRouter')

    CC_ROLES = ['remote']

    def __init__(self, *args):
        super(TaskRouter, self).__init__(*args)
        self.route_map = {}
        self.reply_map = {}

        # 1 hr? XXX
        self.route_lifetime = self.cf.getint ('route-lifetime', 1 * 60 * 60)
        self.reply_timeout = self.cf.getint ('reply-timeout', 5 * 60)
        self.maint_period = self.cf.getint ('maint-period', 1 * 60)

        self.timer = PeriodicCallback(self.do_maint, self.maint_period*1000, self.ioloop)
        self.timer.start()


    def handle_msg(self, cmsg):
        """ Got task from client or reply from TaskRunner / CCTask.
        Dispatch task request to registered TaskRunner.
        Dispatch task reply to requestor (client).
        """

        self.log.trace('got message: %r', cmsg)
        req = cmsg.get_dest()
        sreq = req.split('.')

        if req == 'task.register':
            self.register_host (cmsg)
        elif sreq[:2] == ['task','send']:
            self.send_host (cmsg)
        elif sreq[:2] == ['task','reply']:
            self.send_reply (cmsg)
        else:
            self.log.warning('unknown msg: %s', req)


    def do_maint(self):
        """Drop old routes"""
        self.log.debug('cleanup')
        now = time.time()
        zombies = []
        for hr in self.route_map.itervalues():
            if now - hr.create_time > self.route_lifetime:
                zombies.append(hr)
        for hr in zombies:
            self.log.info('deleting route for %s', hr.host)
            del self.route_map[hr.host]
            self.stat_inc('dropped_routes')

        zombies = []
        for rr in self.reply_map.itervalues():
            if now - rr.atime > self.reply_timeout:
                zombies.append(rr)
        for rr in zombies:
            self.log.info('deleting reply route for %s', rr.uid)
            del self.reply_map[rr.uid]
            self.stat_inc('dropped_tasks')


    def register_host (self, cmsg):
        """Remember ZMQ route for host"""

        route = cmsg.get_route()
        msg = cmsg.get_payload (self.xtx)
        host = msg.host
        self.log.debug ('(%s, %s)', host, route)
        hr = HostRoute (host, route)
        self.route_map[hr.host] = hr

        self.stat_inc ('task.register')

        # FIXME: proper reply?
        #zans = route + [''] + ['OK']
        #self.cclocal.send_multipart(zans)


    def send_host (self, cmsg):
        """Send message for task executor on host"""

        msg = cmsg.get_payload (self.xtx)
        host = msg.task_host

        if host not in self.route_map:
            self.ccerror(cmsg, 'cannot route to %s' % host)
            return

        inr = cmsg.get_route()          # route from/to client
        hr = self.route_map[host]       # find ZMQ route to host
        cmsg.set_route (hr.route)       # re-construct message

        # send the message
        self.log.debug('sending task to %s', host)
        cmsg.send_to (self.cclocal)
        self.stat_inc ('task.send')

        # remember ZMQ route for replies
        req = cmsg.get_dest()
        uid = req.split('.')[2]
        rr = ReplyRoute (uid, inr)
        self.reply_map[uid] = rr

        # send ack to client
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % uid,
                handler = msg['task_handler'],
                task_id = msg['task_id'],
                status = 'forwarded')
        rcm = self.xtx.create_cmsg (rep)
        rcm.set_route (inr)
        rcm.send_to (self.cclocal)

        self.log.debug('saved client for %r', uid)

    def send_reply (self, cmsg):
        """ Send reply message back to task requestor """

        req = cmsg.get_dest()
        uid = req.split('.')[2]

        if uid not in self.reply_map:
            self.log.info ("cannot route back: %s", req)
            return

        self.log.debug ("req: %s", req)

        rr = self.reply_map[uid]        # find ZMQ route
        cmsg.set_route (rr.route)       # re-route message
        cmsg.send_to (self.cclocal)
        rr.atime = time.time()          # update feedback time

        self.stat_inc ('task.reply')

    def ccreply(self, rep, creq):
        crep = self.xtx.create_cmsg(rep)
        crep.take_route(creq)
        crep.send_to(self.cclocal)

    def ccerror(self, cmsg, errmsg):
        self.log.info(errmsg)
        rep = ErrorMessage(msg = errmsg)
        self.ccreply(rep, cmsg)
