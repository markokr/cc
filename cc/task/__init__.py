
import os, os.path
import sys

import skytools

from cc import json
from cc.job import CCJob
from cc.reqs import TaskReplyMessage

def get_task_handlers():
    """Returns list of handler modules.

    Returns dict: (module_name -> docstr)
    """

    d = os.path.dirname(__file__)
    res = {}
    for fn in os.listdir(d):
        if fn[0] in ('.', '_'):
            continue
        name, ext = os.path.splitext(fn)
        if ext == '.py':
            mod = 'cc.task.' + name
            try:
                __import__(mod, level=0)
                m = sys.modules[mod]
                desc = m.__doc__
            except:
                desc = '<broken>'
            res[mod] = desc
    return res

#
# Base class for tasks
#

class CCTask(CCJob):
    """Task base class.

    - loads config & task from stdin as single json blob
    - does not loop by default
    - log is sent into CC
    """
    looping = 0
    task_uid = None
    task_finished = False

    log = skytools.getLogger('t:CCTask')

    def __init__(self, service_name, args):
        info = sys.stdin.read()
        self.task_info = json.Struct.from_json(info)

        super(CCTask, self).__init__(service_name, args)

    def fetch_config(self):
        return self.task_info['config']

    def run(self):
        try:
            CCJob.run(self)
        finally:
            self.send_finished(False)

    def work(self):
        self.connect_cc()
        task = self.task_info['task']
        self.log.info ('got task: %r', task)
        self.task_uid = task['req'].split('.')[2]
        self.process_task(task)

    def process_task(self, task):
        raise NotImplementedError

    def send_feedback (self, fb={}, **kwargs):
        assert isinstance (fb, dict)
        fb = fb or kwargs
        task = self.task_info['task']
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % self.task_uid,
                handler = task['handler'],
                task_id = task['task_id'],
                status = 'feedback',
                feedback = fb)
        self.ccpublish (rep)

    def send_finished (self, ok=True):
        if self.task_finished:
            return
        self.task_finished = True
        stat = ok and 'finished' or 'failed'
        task = self.task_info['task']
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % self.task_uid,
                handler = task['handler'],
                task_id = task['task_id'],
                status = stat)
        self.ccpublish (rep)

    def emit_log(self, rec):
        CCJob.emit_log(self, rec)
        if not self.cc:
            return
        if not self.task_uid:
            return

        # send reply to task client too
        self.send_feedback(
            log_level = rec.levelname,
            service_type = self.service_name,
            job_name = self.job_name,
            log_msg = rec.getMessage(),
            log_time = rec.created,
            log_pid = rec.process,
            log_line = rec.lineno,
            log_function = rec.funcName)
