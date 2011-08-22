
import sys, skytools
from cc import json
from cc.job import CCJob

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
        self.log.info('task work')
        task = self.task_info['task']
        self.process_task(task)

    def process_task(self, tsk):
        pass

