"""Sample task.
"""

import sys
import time

import skytools

from cc.reqs import TaskReplyMessage
from cc.task import CCTask

class SampleTask(CCTask):
    def process_task(self, task):
        uid = task['req'].split('.')[2]
        for i in range(3):
            time.sleep(1)
            rep = TaskReplyMessage(
                    req = 'task.reply.%s' % uid,
                    handler = task['handler'],
                    task_id = task['task_id'],
                    status = 'feedback')
            self.ccpublish (rep)
        # task done
        self.log.info ('task %i done', task['task_id'])
        rep = TaskReplyMessage(
                req = 'task.reply.%s' % uid,
                handler = task['handler'],
                task_id = task['task_id'],
                status = 'finished')
        self.ccpublish (rep)

if __name__ == '__main__':
    t = SampleTask('t:sample', sys.argv[1:])
    t.start()
