""" Sample task with async feedback.
"""

import sys
import threading
import time
import logging
import skytools

from cc.task import CCTask

class SampleTask (CCTask):
    x_time = None

    log = logging.getLogger('cc.task.sample_async')

    def process_task (self, task):
        self.started = time.time()
        self.timer_handler(1)   # launch asynchronous feedback thread
        # execute long running step
        time.sleep(5)
        # task done
        self.timer.cancel()     # stop asynchronous feedback thread
        self.log.info ('task %i done', task['task_id'])
        self.send_finished()

    def timer_handler (self, init = 0):
        t = time.time()
        if not init:
            ed = t - self.x_time
            et = t - self.started
            fb = {'elapsed_delta': ed, 'elapsed_total': et}
            self.send_feedback (fb)
        self.x_time = t
        self.timer = threading.Timer (1, self.timer_handler)
        self.timer.start()

if __name__ == '__main__':
    t = SampleTask('t:sample_async', sys.argv[1:])
    t.start()
