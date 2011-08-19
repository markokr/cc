"""Sample task.
"""

import sys, skytools
from cc.task import CCTask

class SampleTask(CCTask):
    def process_task(self, task):
        self.log.info('task: %r', task)

if __name__ == '__main__':
    t = SampleTask('sample', sys.argv[1:])
    t.start()

