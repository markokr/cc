
import zmq
import zmq.eventloop
import zmq.eventloop.zmqstream
import json
import skytools

__all__ = ['HotsScript', 'HotsDBScript']

sock_type_map = {
    'pub': zmq.PUB,
    'sub': zmq.SUB,
    'req': zmq.REQ,
    'rep': zmq.REP,
    'pull': zmq.PULL,
    'push': zmq.PUSH,
    'xreq': zmq.XREQ,
    'xrep': zmq.XREP,
    'xpub': zmq.XPUB,
    'xsub': zmq.XSUB,
}

class HotsScriptMethods(object):
    zsocket_cache = None
    zctx = None
    zpoller = None
    log = None
    ioloop = None

    def get_socket(self, sock_name, default_url=None):
        # initialize
        if not self.zctx:
            self.zctx = zmq.Context()
        if self.zsocket_cache is None:
            self.zsocket_cache = {}

        # find socket in cache
        if sock_name in self.zsocket_cache:
            self.log.debug('Cached socket req: %s' % (sock_name,))
            return self.zsocket_cache[sock_name]

        # parse name
        btype, stype, sname = sock_name.split('-', 2)
        url = self.cf.get(sock_name, default_url)
        self.log.debug('New socket req: %s [%s]' % (sock_name, url))
        if not url:
            raise skytools.UsageError('Socket %s not configured' % sock_name)
        if btype not in ('local', 'remote'):
            raise skytools.UsageError('Socket %s has unknown bind type [%s], excepted: [local,remote]' % (
                                      sock_name, btype))
        if stype not in sock_type_map:
            raise skytools.UsageError('Socket %s has unknown type [%s], excepted: [%s]' % (
                                      sock_name, stype, ",".join(sock_type_map.keys())))

        # create socket
        s = self.zctx.socket(sock_type_map[stype])
        if btype == 'local':
            s.bind(url)
        else:
            s.connect(url)
        self.zsocket_cache[sock_name] = s
        return s

    def get_stream(self, sock_name, default_url=None):
        if self.ioloop is None:
            self.ioloop = zmq.eventloop.IOLoop.instance()
        sock = self.get_socket(sock_name, default_url)
        return zmq.eventloop.zmqstream.ZMQStream(sock, self.ioloop)

    def get_poller(self):
        if self.zsocket_cache is None:
            self.zsocket_cache = {}
        if self.zpoller is None:
            self.zpoller = zmq.Poller()
            for s in self.zsocket_cache.values():
                self.zpoller.register(s, zmq.POLLIN)
        return self.zpoller

    def zpoller_register(self, sock_name, handler):
        pass

class HotsScript(HotsScriptMethods, skytools.BaseScript):
    pass

class HotsDBScript(HotsScriptMethods, skytools.DBScript):
    pass

