"""Send requests to database function.

Handler sets up a ZMQ socket on random port
where workers connect to and receive messages.

"""

import re
import threading
import time
from types import *

import skytools
import zmq

import cc.json
from cc.handler.proxy import BaseProxyHandler
from cc.message import CCMessage
from cc.reqs import parse_json, ReplyMessage

__all__ = ['DBHandler']

CC_HANDLER = 'DBHandler'

#
# worker thread
#

class DBWorker(threading.Thread):
    """Worker thread, can do blocking calls."""

    log = skytools.getLogger('h:DBWorker')

    def __init__(self, name, xtx, zctx, url, connstr, func_list):
        super(DBWorker, self).__init__(name=name)
        self.log = skytools.getLogger (name)
        self.xtx = xtx
        self.zctx = zctx
        self.master_url = url
        self.connstr = connstr
        self.func_list = func_list
        self.db = None
        self.master = None
        self.looping = True

    def startup (self):
        self.master = self.zctx.socket (zmq.XREP)
        self.master.connect (self.master_url)
        self.poller = zmq.Poller()
        self.poller.register (self.master, zmq.POLLIN)

    def run (self):
        self.log.info ("%s running", self.name)
        self.startup()
        while self.looping:
            try:
                self.work()
            except:
                self.log.exception('worker crash, dropping msg')
                self.reset()
                time.sleep(10)
        self.shutdown()

    def reset(self):
        try:
            if self.db:
                self.db.close()
        except:
            pass
        self.db = None

    def stop (self):
        self.looping = False

    def shutdown (self):
        self.log.info ("%s stopping", self.name)
        self.reset()

    def work(self):
        socks = dict (self.poller.poll (1000))
        if self.master in socks and socks[self.master] == zmq.POLLIN:
            zmsg = self.master.recv_multipart()
        else: # timeout
            return
        try:
            cmsg = CCMessage (zmsg)
            self.log.trace ('%s', cmsg)
        except:
            self.log.exception ("invalid CC message")
            return

        if not self.db:
            self.log.info('connecting to database')
            self.db = skytools.connect_database(self.connstr)
            self.db.set_isolation_level(0)

        self.process_request(cmsg)

    def process_request(self, cmsg):
        msg = cmsg.get_payload(self.xtx)
        if not msg:
            return
        curs = self.db.cursor()
        func = msg.function
        args = msg.get ('params', [])
        if isinstance (args, StringType):
            args = cc.json.loads (args)
        assert isinstance (args, (DictType, ListType, TupleType))

        if len(self.func_list) == 1 and self.func_list[0] == '*':
            pass
        elif func in self.func_list:
            pass
        else:
            self.log.error('Function call not allowed: %r', func)
            return None

        q = "select %s (%%s)" % (skytools.quote_fqident(func),)
        if isinstance (args, DictType):
            if not all ([re.match ("^[a-zA-Z0-9_]+$", k) for k in args.keys()]):
                self.log.error ("Invalid DB function argument name in %r", args.keys())
                return
            q %= (", ".join(["%s := %%(%s)s" % (k,k) for k in args.keys()]),)
        else:
            q %= (", ".join(["%s" for a in args]),)
        if self.log.isEnabledFor (skytools.skylog.TRACE):
            self.log.trace ('Executing: %s', curs.mogrify (q, args))
        else:
            self.log.debug ('Executing: %s', q)
        curs.execute (q, args)

        rt = msg.get ('return')
        if rt in (None, '', 'no'):
            return
        elif rt == 'all':
            rs = curs.fetchall()
        elif rt == 'one':
            rs = curs.fetchone()
        elif rt == 'json':
            rs = curs.fetchone()
            if rs:
                jsr = rs[0]
            else:
                jsr = '{}'
            rep = parse_json (jsr)
        if rt != 'json':
            rep = ReplyMessage(
                    req = "reply.%s" % msg.req,
                    data = rs)
            if curs.rowcount >= 0:
                rep.rowcount = curs.rowcount
            if curs.statusmessage:
                rep.statusmessage = curs.statusmessage
            if msg.get('ident'):
                rep.ident = msg.get('ident')

        rcm = self.xtx.create_cmsg (rep)
        rcm.take_route (cmsg)
        rcm.send_to (self.master)

#
# db proxy
#

class DBHandler (BaseProxyHandler):
    """Send request to workers."""

    CC_ROLES = ['remote']

    log = skytools.getLogger('h:DBHandler')

    def startup (self):
        super(DBHandler, self).startup()
        self.workers = []

    def make_socket (self):
        """ Create socket for sending msgs to workers. """
        url = 'inproc://workers'
        sock = self.zctx.socket (zmq.XREQ)
        port = sock.bind_to_random_port (url)
        self.worker_url = "%s:%d" % (url, port)
        return sock

    def launch_workers(self):
        """ Create and start worker threads. """
        nworkers = self.cf.getint('worker-threads', 10)
        func_list = self.cf.getlist('allowed-functions', [])
        self.log.info('allowed-functions: %r', func_list)
        connstr = self.cf.get('db')
        for i in range(nworkers):
            wname = "%s.worker-%i" % (self.hname, i)
            self.log.info ('starting %s', wname)
            w = DBWorker(
                    wname, self.xtx, self.zctx, self.worker_url,
                    connstr, func_list)
            self.workers.append (w)
            w.start()

    def stop (self):
        """ Signal workers to shut down. """
        super(DBHandler, self).stop()
        self.log.info ("stopping")
        for w in self.workers:
            self.log.info ("signalling %s", w.name)
            w.stop()
