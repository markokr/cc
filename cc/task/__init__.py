
import sys

import skytools

from cc import json
from cc.job import CCJob
from cc.reqs import TaskReplyMessage

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
    def __init__(self, service_name, args):
        info = sys.stdin.read()
        self.task_info = json.Struct.from_json(info)

        super(CCTask, self).__init__(service_name, args)

    def fetch_config(self):
        return self.task_info['config']

    def work(self):
        self.connect_cc()
        task = self.task_info['task']
        self.log.info ('CCTask.work: %r', task)
        self.task_uid = task['req'].split('.')[2]
        self.process_task(task)

    def process_task(self, task):
        raise NotImplementedError

    def send_feedback (self, fb):
        assert isinstance (fb, dict)
        task = self.task_info['task']
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % self.task_uid,
                handler = task['handler'],
                task_id = task['task_id'],
                status = 'feedback',
                feedback = fb)
        self.ccpublish (rep)

    def send_finished (self):
        task = self.task_info['task']
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % self.task_uid,
                handler = task['handler'],
                task_id = task['task_id'],
                status = 'finished')
        self.ccpublish (rep)
