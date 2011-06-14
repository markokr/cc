from cc.json import Struct, Field

class BaseMessage(Struct):
    req = Field(str)

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

class JobRequest(BaseMessage):
    "job.*"
    job_name = Field(str)

class JobConfigReply(BaseMessage):
    "job.config"
    job_name = Field(str)
    config = Field(dict)

class TaskRegister(BaseMessage):
    "req.task.register"
    host = Field(str)

class TaskSend(BaseMessage):
    "req.task.send"
    host = Field(str)



def parse_json(js):
    return Struct.from_json(js)

