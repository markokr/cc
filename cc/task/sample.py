"""Sample task.
"""

import sys
import time

import skytools

from cc.task import CCTask

class SampleTask(CCTask):

    def fetch_config(self):

        # crash before daemonizing if requested
        t = self.task_info['task']
        if t['cmd'] == 'crash-launch':
            raise Exception('launch failed')

        return CCTask.fetch_config(self)

    def process_task(self, task):

        # crash during run
        if task['cmd'] == 'crash-run':
            raise Exception('run failed')

        # do some work
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
