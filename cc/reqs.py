import socket
import time

from cc.json import Struct, Field
from cc.message import CCMessage

__all__ = ['LogMessage', 'InfofileMessage', 'JobRequestMessage', 'JobConfigReplyMessage', 'TaskRegisterMessage', 'TaskSendMessage']

class BaseMessage(Struct):
    req = Field(str)
    hostname = Field(str, default = socket.gethostname())

class ErrorMessage (BaseMessage):
    msg = Field(str)

class LogMessage(BaseMessage):
    "log.*"
    level = Field(str)
    service_type = Field(str)
    job_name = Field(str)
    msg = Field(str)
    time = Field(float)
    pid = Field(int)
    line = Field(int)
    function = Field(str)

class InfofileMessage(BaseMessage):
    "pub.infofile"
    mtime = Field(float)                # last modification time of file
    filename = Field(str)
    data = Field(str)                   # file contents (data fork)
    comp = Field(str)                   # compression method used
    mode = Field(str)                   # file mode to use for fopen

class JobConfigRequestMessage(BaseMessage):
    "job.config"
    job_name = Field(str)

class JobConfigReplyMessage(BaseMessage):
    "job.config"
    job_name = Field(str)
    config = Field(dict)

class TaskRegisterMessage(BaseMessage):
    "req.task.register"
    host = Field(str)

class TaskSendMessage(BaseMessage):
    "req.task.send"
    host = Field(str)
    handler = Field(str)
    task_id = Field(int)

class TaskReplyMessage (BaseMessage):
    "req.task.reply"
    handler = Field(str)
    task_id = Field(int)
    status = Field(str)


def parse_json(js):
    return Struct.from_json(js)
