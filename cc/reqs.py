import time
from cc.json import Struct, Field
from cc.message import CCMessage

__all__ = ['LogMessage', 'InfofileMessage', 'JobRequestMessage', 'JobConfigReplyMessage', 'TaskRegisterMessage', 'TaskSendMessage']

class BaseMessage(Struct):
    req = Field(str)

    def send_to(self, sock):
        cmsg = CCMessage(jmsg = self)
        sock.send_multipart(cmsg.zmsg)

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
    mtime = Field(float)
    filename = Field(str)
    body = Field(str)

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



def parse_json(js):
    return Struct.from_json(js)

