"""Client side task handling.
"""

import uuid
import logging
from zmq.eventloop import IOLoop
from cc.stream import CCReqStream
from cc.reqs import TaskSendMessage

class TaskInfo:
    """Per-task state, replies."""
    log = logging.getLogger('cc.taskmgr.TaskInfo')
    def __init__(self, task, task_cbfunc, ccrq):
        self.task = task
        self.uuid = task['task_id']
        self.task_cbfunc = task_cbfunc
        self.replies = []
        self.ccrq = ccrq
        self.retry_count = 3
        self.retry_timeout = 15
        self.query_id = None

    def send_task(self):
        """Send the task away."""
        self.log.debug('TaskInfo.send_task')
        self.query_id = self.ccrq.ccquery_async(self.task, self.process_reply, self.retry_timeout)

    def process_reply(self, msg):
        """Main processing logic.

        If msg==None, then timeout occured.

        Returns tuple of (keep, timeout) to CCReqStream.
        """

        if msg is None:
            if self.retry_count > 0:
                self.log.warning('timeout, resending')
                self.retry_count -= 1
                self.ccrq.resend(self.query_id)
                return (True, self.retry_timeout)
            self.log.error('TaskInfo.process_reply: timeout, task failed')
            self.task_cbfunc(True, msg)
            return (False, 0)

        tup = msg.req.split('.')
        if tup[0] == 'error':
            done = True
            self.log.error('got error: %r', msg)
        elif tup[:2] == ['task', 'reply']:
            done = msg.status in ('finished', 'failed', 'stopped')
            self.log.debug('got result: %r', msg)
        else:
            done = False
            self.log.debug('got random: %r', msg)
        self.replies.append(msg)
        self.task_cbfunc(done, msg)
        self.log.debug('TaskInfo.process_reply: done=%r', done)
        if done:
            return (False, 0)
        return (True, 0)

class TaskManager:
    """Manages task on single CCReqStream connection."""

    log = logging.getLogger('cc.taskmgr.TaskManager')

    def __init__(self, ccrq):
        self.ccrq = ccrq
        self.ioloop = ccrq.ioloop

    def send_task_async(self, task, task_cbfunc):
        """Async task launch.

        Callback function will be called on replies.

        @param task: TaskSendMessage
        @param task_cbfunc: func with args (is_done, reply_msg)
        """
        assert isinstance(task, TaskSendMessage)

        self.log.debug('TaskManager.send_task_async(%r, %r)', task, task_cbfunc)
        ti = TaskInfo(task, task_cbfunc, self.ccrq)
        ti.send_task()
        return ti

    def send_task(self, task):
        """Sync task launch.
        
        Returns TaskInfo with replies when finished.
        """
        assert isinstance(task, TaskSendMessage)

        self.log.debug('TaskManager.send_task(%r, %r)', task)
        def cb(done, rep):
            if done:
                self.ioloop.stop()
        ti = self.send_task_async(task, cb)
        self.ioloop.start()
        return ti

