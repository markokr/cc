
"""
CC daemon / task
"""

import logging
import socket

import zmq

import skytools

from cc import json
from cc.crypto import CryptoContext
from cc.message import CCMessage
from cc.reqs import BaseMessage, JobConfigRequestMessage, LogMessage

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


class CCJob(skytools.DBScript):
    zctx = None
    cc = None

    def __init__(self, service_type, args):
        # no crypto for logs
        self.logxtx = CryptoContext(None)
        self.xtx = CryptoContext(None)

        super(CCJob, self).__init__(service_type, args)

        self.hostname = socket.gethostname()

        root = logging.getLogger()
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

    def fetch_config(self):
        """ Query config """
        msg = JobConfigRequestMessage (job_name = self.job_name)
        rep = self.ccquery(msg)
        cf = rep.config
        cf['use_skylog'] = '0'
        return rep.config

    def load_config(self):
        """Loads and returns skytools.Config instance.

        By default it uses first command-line argument as config
        file name.  Can be overrided.
        """

        if self.options.ccdaemon:
            self.job_name = self.options.ccdaemon
        elif self.options.cctask:
            self.job_name = self.options.cctask
        else:
            raise skytools.UsageError('Need either --cctask or --ccdaemon')

        conf = self.fetch_config()
        return skytools.Config(self.service_name, None, user_defs = conf)

    def _boot_daemon(self):
        # close ZMQ context/thread before forking to background
        self.close_cc()

        super(CCJob, self)._boot_daemon()

    def connect_cc(self):
        if not self.zctx:
            self.zctx = zmq.Context()
        if not self.cc:
            url = self.options.cc
            self.cc = self.zctx.socket(zmq.XREQ)
            self.cc.connect(url)
            self.cc.setsockopt(zmq.LINGER, 500)
        return self.cc

    def close_cc(self):
        if self.cc:
            self.cc.close()
            self.cc = None
        if self.zctx:
            self.zctx.term()
            self.zctx = None

    def init_optparse(self, parser = None):
        p = super(CCJob, self).init_optparse(parser)

        p.add_option("--cc", help = "master CC url")
        p.add_option("--ccdaemon", help = "daemon name")
        p.add_option("--cctask", help = "task id")

        return p

    def stat_inc(self, key, increase = 1):
        return self.stat_increase (key, increase)
