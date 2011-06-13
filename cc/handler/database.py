
import zmq, threading

from cc.handler.proxy import ProxyHandler
from cc.message import CCMessage

import skytools


__all__ = ['DBHandler']

CC_HANDLER = 'DBHandler'

#
# db proxy
#

def db_worker(zctx, worker_url, connstr):
    """Worker thread, can do blocking calls."""
    s = zctx.socket(zmq.REP)
    s.connect(worker_url)
    while 1:
        cmsg = s.recv_cmsg()
        s.send_multipart(['fooz', '{fooz}'])

class DBHandler(ProxyHandler):
    """Send request to workers."""
    def make_socket(self):
        baseurl = 'tcp://127.0.0.1'
        s = self.zctx.socket(zmq.XREQ)
        port = s.bind_to_random_port('tcp://127.0.0.1')
        self.worker_url = "%s:%d" % (baseurl, port)
        return s

    def launch_workers(self):
        nworkers = 10
        wargs = (self.zctx, self.worker_url, self.cf.get('db'))
        for i in range(nworkers):
            threading.Thread(target = db_worker, args = wargs)

