
"""
CC daemon / task
"""

import sys, os, signal, optparse, time, errno, select
import logging, logging.handlers, logging.config

import skytools
import skytools.skylog

import zmq

from cc import json
from cc.message import CCMessage
from cc.stream import CCStream

class CallbackLogger(logging.Handler):
    def __init__(self, cbfunc):
        logging.Handler.__init__(self)
        self.log_cb = cbfunc

    def emit(self, rec):
        self.log_cb(rec)

class CCJob(skytools.BaseScript):
    zctx = None
    cc = None

    def __init__(self, service_type, args):
        super(CCJob, self).__init__(service_type, args)

        self.log.addHandler(CallbackLogger(self.emit_log))

    def emit_log(self, rec):
        if not self.cc:
            return
        jsrec = {
            'req': 'log.%s' % rec.levelname.lower(),
            'level': rec.levelname,
            'service_type': self.service_name,
            'job_name': self.job_name,
            'msg': rec.getMessage(),
            'time': rec.created,
            'pid': rec.process,
            'line': rec.lineno,
            'function': rec.funcName,
        }
        zmsg = ['', jsrec['req'], json.dumps(jsrec), '']
        self.cc.send_multipart(zmsg)

    def ccquery(self, req, **kwargs):
        q = kwargs.copy()
        q['req'] = req
        if not self.cc:
            self.connect_cc()
        zmsg = ['<query-id>', '', q['req'], json.dumps(q), '']
        
        self.cc.send_multipart(zmsg)
        res = self.cc.recv_multipart()
        cmsg = CCMessage(res)
        return cmsg

    def ccpublish(self, req, **kwargs):
        q = kwargs.copy()
        q['req'] = req
        if not self.cc:
            self.connect_cc()
        zmsg = ['', q['req'], json.dumps(q), '']
        self.cc.send_multipart(zmsg)

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
        cmsg = self.ccquery('job.config', job_name = self.job_name)
        #self.log.debug('got config: %s', cmsg)
        conf = cmsg.get_payload()['config']

        return skytools.Config(self.service_name, None, user_defs = conf, override = self.cf_operride)

    def _boot_daemon(self):
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

