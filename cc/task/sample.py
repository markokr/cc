"""Sample task.
"""

import sys
import time

import skytools

from cc.task import CCTask

class SampleTask(CCTask):
    def process_task(self, task):
        for i in range(3):
            time.sleep(1)
            fb = {'i': i}
            self.send_feedback (fb)
        # task done
        self.log.info ('task %i done', task['task_id'])
        self.send_finished()

if __name__ == '__main__':
    t = SampleTask('t:sample', sys.argv[1:])
    t.start()
