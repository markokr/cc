
"""
CC daemon / task
"""

import logging
import socket
import sys

import skytools
import zmq

from cc import json
from cc.crypto import CryptoContext
from cc.message import CCMessage
from cc.reqs import BaseMessage, JobConfigRequestMessage, LogMessage
from cc.util import hsize_to_bytes

__all__ = ['CCJob', 'CCDaemon', 'CCTask']


class CallbackLogger(logging.Handler):
    """Call a function on log event."""
    def __init__(self, cbfunc):
        logging.Handler.__init__(self)
        self.log_cb = cbfunc

    def emit(self, rec):
        self.log_cb(rec)


class NoLog:
    def debug(self, *args): pass
    def info(self, *args): pass
    def warning(self, *args): pass
    def error(self, *args): pass
    def critical(self, *args): pass


def make_job_defaults(main_cf, job_service_name):
    cc_jobname = main_cf.get('job_name')
    defs = {}
    defs['use_skylog'] = '0'
    defs['service_name'] = job_service_name
    defs['job_name'] = "%s_%s" % (cc_jobname, job_service_name)
    if main_cf.has_option('pidfile'):
        defs['pidfile'] = main_cf.cf.get('ccserver', 'pidfile', raw=True)
    return defs


class CCJob(skytools.DBScript):
    zctx = None
    cc = None
    cc_url = None

    zmq_nthreads = 1
    zmq_linger = 500
    zmq_hwm = 100
    zmq_rcvbuf = 0 # means no change
    zmq_sndbuf = 0 # means no change

    def __init__(self, service_type, args):
        # no crypto for logs
        self.logxtx = CryptoContext(None)
        self.xtx = CryptoContext(None)

        super(CCJob, self).__init__(service_type, args)

        self.hostname = socket.gethostname()

        root = skytools.getLogger()
        root.addHandler(CallbackLogger(self.emit_log))

        self.xtx = CryptoContext(self.cf)

    def emit_log(self, rec):
        if not self.cc:
            self.connect_cc()
        if not self.cc:
            return
        msg = LogMessage(
            req = 'log.%s' % rec.levelname.lower(),
            log_level = rec.levelname,
            service_type = self.service_name,
            job_name = self.job_name,
            log_msg = rec.getMessage(),
            log_time = rec.created,
            log_pid = rec.process,
            log_line = rec.lineno,
            log_function = rec.funcName)
        cmsg = self.logxtx.create_cmsg(msg)
        cmsg.send_to (self.cc)

    def ccquery (self, msg, blob = None):
        """Sends query to CC, waits for answer."""
        assert isinstance (msg, BaseMessage)
        if not self.cc: self.connect_cc()

        cmsg = self.xtx.create_cmsg (msg, blob)
        cmsg.send_to (self.cc)

        crep = CCMessage(self.cc.recv_multipart())
        return crep.get_payload(self.xtx)

    def ccpublish (self, msg, blob = None):
        assert isinstance (msg, BaseMessage)
        if not self.cc:
            self.connect_cc()
        cmsg = self.xtx.create_cmsg (msg, blob)
        cmsg.send_to (self.cc)

    def load_config(self):
        """Loads and returns skytools.Config instance.

        By default it uses first command-line argument as config
        file name.  Can be overrided.
        """

        cf = skytools.Config('ccserver', self.args[0])
        self.cc_jobname = cf.get('job_name')
        self.cc_url = cf.get('cc-socket')

        if len(self.args) > 1:
            self.service_name = self.args[1]

        self.cf_defaults = make_job_defaults(cf, self.service_name)

        cf = super(CCJob, self).load_config()
        return cf

    def reload (self):
        super(CCJob, self).reload()

        self.zmq_nthreads = self.cf.getint ('zmq_nthreads', self.zmq_nthreads)
        self.zmq_hwm = self.cf.getint ('zmq_hwm', self.zmq_hwm)
        self.zmq_linger = self.cf.getint ('zmq_linger', self.zmq_linger)
        self.zmq_rcvbuf = hsize_to_bytes (self.cf.get ('zmq_rcvbuf', str(self.zmq_rcvbuf)))
        self.zmq_sndbuf = hsize_to_bytes (self.cf.get ('zmq_sndbuf', str(self.zmq_sndbuf)))

    def _boot_daemon(self):
        # close ZMQ context/thread before forking to background
        self.close_cc()

        super(CCJob, self)._boot_daemon()

    def connect_cc(self):
        if not self.zctx:
            self.zctx = zmq.Context(self.zmq_nthreads)
        if not self.cc:
            url = self.cc_url
            self.cc = self.zctx.socket(zmq.XREQ)
            self.cc.setsockopt(zmq.LINGER, self.zmq_linger)
            self.cc.setsockopt(zmq.HWM, self.zmq_hwm)
            if self.zmq_rcvbuf > 0:
                s.setsockopt (zmq.RCVBUF, self.zmq_rcvbuf)
            if self.zmq_sndbuf > 0:
                s.setsockopt (zmq.SNDBUF, self.zmq_sndbuf)
            self.cc.connect(url)
        return self.cc

    def close_cc(self):
        if self.cc:
            self.cc.close()
            self.cc = None
        if self.zctx:
            self.zctx.term()
            self.zctx = None
