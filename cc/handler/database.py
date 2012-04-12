"""Send requests to database function.

Handler sets up a ZMQ socket on random port
where workers connect to and receive messages.

"""

import threading
import time

import skytools
import zmq

from cc.handler.proxy import ProxyHandler
from cc.message import CCMessage
from cc.reqs import parse_json
from cc.stream import CCStream

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

    def run(self):
        self.log.debug ('%s running', self.name)
        self.master = self.zctx.socket (zmq.XREP)
        self.master.connect (self.master_url)
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
        zmsg = self.master.recv_multipart()
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
        args = msg.get('payload','{}')
        js = args.dump_json()

        if len(self.func_list) == 1 and self.func_list[0] == '*':
            pass
        elif func in self.func_list:
            pass
        else:
            self.log.error('Function call not allowed: %r', func)
            return None

        q = "select %s(%%s)" % skytools.quote_fqident(func)
        if self.log.isEnabledFor (skytools.skylog.TRACE):
            self.log.trace ('Executing: %s', curs.mogrify (q, [js]))
        else:
            self.log.debug ('Executing: %s', q)
        curs.execute(q, [js])

        res = curs.fetchall()
        if not res:
            jsr = '{}'
        else:
            jsr = res[0][0]
        jmsg = parse_json(jsr)
        rcm = self.xtx.create_cmsg (jmsg)
        rcm.take_route (cmsg)
        rcm.send_to (self.master)

#
# db proxy
#

class DBHandler(ProxyHandler):
    """Send request to workers."""

    CC_ROLES = ['remote']

    log = skytools.getLogger('h:DBHandler')

    def make_socket (self):
        """ Create socket for sending msgs to workers. """
        url = 'tcp://127.0.0.1' # 'inproc://workers'
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
            w.start()
