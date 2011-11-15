"""Send requests to database function.

Handler sets up a ZMQ socket on random port
where workers connect to and receive messages.

"""

import logging
import threading
import time

import skytools
import zmq

from cc.handler.proxy import ProxyHandler
from cc.message import CCMessage
from cc.handler import CCHandler
from cc.stream import CCStream
from cc.job import CallbackLogger
from cc.reqs import parse_json
from cc.crypto import CryptoContext


__all__ = ['DBHandler']

CC_HANDLER = 'DBHandler'

#
# worker thread
#

class DBWorker(threading.Thread):
    """Worker thread, can do blocking calls."""

    log = logging.getLogger('h:DBWorker')

    def __init__(self, name, zctx, worker_url, connstr, func_list, xtx):
        super(DBWorker, self).__init__(name=name)
        self.zctx = zctx
        self.worker_url = worker_url
        self.connstr = connstr
        self.db = None
        self.wconn = None
        self.func_list = func_list
        self.xtx = xtx

    def run(self):
        self.log.debug('worker running')
        self.wconn = self.zctx.socket(zmq.XREP)
        self.wconn.connect(self.worker_url)
        while 1:
            try:
                self.work()
            except:
                self.log.exception('worker crash, dropping msg')
                self.reset()
                time.sleep(10)

    def reset(self):
        try:
            if self.db:
                self.db.close()
                self.db = None
        except:
            pass

    def work(self):
        zmsg = self.wconn.recv_multipart()
        cmsg = CCMessage(zmsg)
        self.log.debug('DBWorker: msg=%r', cmsg)

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
        js = cmsg.get_payload_json()

        func = msg.function

        if len(self.func_list) == 1 and self.func_list[0] == '*':
            pass
        elif func in self.func_list:
            pass
        else:
            self.log.error('Function call not allowed: %r', func)
            return None

        q = "select %s(%%s)" % skytools.quote_fqident(func)
        self.log.debug('Executing: %s', q)
        curs.execute(q, [js])

        res = curs.fetchall()
        if not res:
            js2 = '{}'
        else:
            js2 = res[0][0]
        jmsg = parse_json(js2)
        cmsg2 = CCMessage(jmsg = jmsg)
        cmsg2.take_route(cmsg)
        cmsg2.send_to (self.wconn)


#
# db proxy
#

class DBHandler(ProxyHandler):
    """Send request to workers."""

    CC_ROLES = ['remote']

    log = logging.getLogger('h:DBHandler')

    def make_socket(self):
        baseurl = 'tcp://127.0.0.1'
        s = self.zctx.socket(zmq.XREQ)
        port = s.bind_to_random_port('tcp://127.0.0.1')
        self.worker_url = "%s:%d" % (baseurl, port)
        return s

    def launch_workers(self):
        nworkers = self.cf.getint('worker-threads', 10)
        func_list = self.cf.getlist('allowed-functions', [])
        self.log.info('allowed-functions: %r', func_list)
        connstr = self.cf.get('db')
        for i in range(nworkers):
            wname = '.worker%d' % i
            self.log.info('launching: %s.%s', self.hname, wname)
            w = DBWorker(self.hname + '.' + wname, self.zctx, self.worker_url,
                         connstr, func_list, self.xtx)
            w.start()
