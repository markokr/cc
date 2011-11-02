import socket
import time

from cc.json import Struct, Field
from cc.message import CCMessage

__all__ = ['LogMessage', 'InfofileMessage', 'JobRequestMessage', 'JobConfigReplyMessage', 'TaskRegisterMessage', 'TaskSendMessage']

class BaseMessage(Struct):
    # needs default as json.py seems to get inheritance wrong
    req = Field(str, '?')

    time = Field(float, default = time.time)
    hostname = Field(str, default = socket.gethostname())
    #blob_hash = Field(str, default = '')

class ReplyMessage (BaseMessage):
    req = Field(str, "reply")

class ErrorMessage (ReplyMessage):
    req = Field(str, "error")
    msg = Field(str)

class LogMessage(BaseMessage):
    "log.*"
    log_level = Field(str)
    service_type = Field(str)
    job_name = Field(str)
    log_msg = Field(str)
    log_time = Field(float)
    log_pid = Field(int)
    log_line = Field(int)
    log_function = Field(str)

class InfofileMessage(BaseMessage):
    req = Field(str, "pub.infofile")
    mtime = Field(float)                # last modification time of file
    filename = Field(str)
    data = Field(str)                   # file contents (data fork)
    comp = Field(str)                   # compression method used
    mode = Field(str, 'b')              # file mode to use for fopen

class LogtailMessage (BaseMessage):
    req = Field(str, "pub.logtail")
    filename = Field(str)
    data = Field(str)                   # file contents (data fork)
    mode = Field(str, 'b')              # file mode to use for fopen

class JobConfigRequestMessage(BaseMessage):
    req = Field(str, "job.config")
    job_name = Field(str)

class JobConfigReplyMessage(BaseMessage):
    req = Field(str, "job.config")
    job_name = Field(str)
    config = Field(dict)

class TaskRegisterMessage(BaseMessage):
    req = Field(str, "task.register")
    host = Field(str)

class TaskSendMessage(BaseMessage):
    """Request to execute a task"""
    req = Field(str, "task.send")
    task_host = Field(str)
    task_handler = Field(str)
    task_id = Field(str)

class TaskReplyMessage (BaseMessage):
    req = Field(str, "task.reply")
    task_id = Field(str)
    status = Field(str) # launched, feedback, finished, failed, running, stopped
    #feedback = Field(dict)


def parse_json(js):
    return Struct.from_json(js)
