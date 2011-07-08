
"""
CC daemon / task
"""

import logging
import zmq
import socket

from cc import json
from cc.message import CCMessage

from cc.reqs import JobConfigRequestMessage, JobConfigReplyMessage, LogMessage, BaseMessage

from cc.crypto import CryptoContext

import skytools

__all__ = ['CCJob', 'CCDaemon', 'CCTask']

class CallbackLogger(logging.Handler):
    """Call a function on log event."""
    def __init__(self, cbfunc):
        logging.Handler.__init__(self)
        self.log_cb = cbfunc

    def emit(self, rec):
        self.log_cb(rec)

class CCJob(skytools.BaseScript):
    zctx = None
    cc = None

    def __init__(self, service_type, args):
        self.xtx = CryptoContext(None)
        super(CCJob, self).__init__(service_type, args)

        self.hostname = socket.gethostname()

        self.log.addHandler(CallbackLogger(self.emit_log))

        self.xtx = CryptoContext(self.cf)

    def emit_log(self, rec):
        if not self.cc:
            return
        msg = LogMessage(
            req = 'log.%s' % rec.levelname.lower(),
            level = rec.levelname,
            service_type = self.service_name,
            job_name = self.job_name,
            msg = rec.getMessage(),
            time = rec.created,
            pid = rec.process,
            line = rec.lineno,
            function = rec.funcName)
        self.ccpublish(msg)

    def ccquery(self, msg):
        """Sends query to CC, waits for answer."""
        if not self.cc:
            self.connect_cc()

        cmsg = self.xtx.create_cmsg(msg)
        self.cc.send_multipart(cmsg.zmsg)

        crep = CCMessage(self.cc.recv_multipart())
        return crep.get_payload(self.xtx)

    def ccpublish(self, msg):
        assert isinstance(msg, BaseMessage)
        if not self.cc:
            self.connect_cc()
        cmsg = self.xtx.create_cmsg(msg)
        self.cc.send_multipart(cmsg.zmsg)

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

        # query config
        msg = JobConfigRequestMessage(
                req = 'job.config',
                job_name = self.job_name)
        rep = self.ccquery(msg)
        conf = rep.config
        return skytools.Config(self.service_name, None, user_defs = conf,
                               override = self.cf_operride)

    def _boot_daemon(self):
        # close ZMQ context/thread before forking to background
        self.close_cc()

        super(CCJob, self)._boot_daemon()

    def connect_cc(self):
        if not self.zctx:
            self.zctx = zmq.Context()
        if not self.cc:
            url = self.options.cc or 'tcp://127.0.0.1:10000'
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
        """Increases a stat value."""
        if key in self.stat_dict:
            self.stat_dict[key] += increase
        else:
            self.stat_dict[key] = increase

    def set_state(self, key, increase = 1):
        """Increases a stat value."""
        if key in self.stat_dict:
            self.stat_dict[key] += increase
        else:
            self.stat_dict[key] = increase

