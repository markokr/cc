
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
    'pair': zmq.PAIR,
    'dealer': zmq.XREQ,
    'router': zmq.XREP,

    # undocumented?
    'xpub': zmq.XPUB,
    'xsub': zmq.XSUB,

    # obsolete
    'xreq': zmq.XREQ,
    'xrep': zmq.XREP,
}

class HotsScriptMethods(object):
    zsocket_cache = None
    zstream_cache = None
    zctx = None
    zpoller = None
    log = None
    ioloop = None

    def get_socket(self, sock_name, default_url=None):
        """Returns cached zmq.Socket.

        Socket is initialized on first call.

        Socket url is loaded from config value.

        Socket name determines the type and whether to connect
        or bind it:

          (remote|local)-(sub|pub|pull|push|..)-description

        Eg. 'remote-pub-log' is zmq.PUB socket, and needs .connect().
        """

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
        """Returns cached ZMQStream"""
        if self.ioloop is None:
            self.ioloop = zmq.eventloop.IOLoop.instance()
        if self.zstream_cache is None:
            self.zstream_cache = {}
        if sock_name in self.zstream_cache:
            return self.zstream_cache[sock_name]
        sock = self.get_socket(sock_name, default_url)
        stream = zmq.eventloop.zmqstream.ZMQStream(sock, self.ioloop)
        self.zstream_cache[sock_name] = stream
        return stream

    def work(self):
        """Default work loop simply runs ioloop."""
        if self.ioloop is None:
            raise skytools.UsageError('No sockets registered...')

        self.log.info('Starting ioloop')
        self.ioloop.start()
        return 1

class HotsScript(HotsScriptMethods, skytools.BaseScript):
    """ZMQ script without db."""

class HotsDBScript(HotsScriptMethods, skytools.DBScript):
    """ZMQ script with db"""

