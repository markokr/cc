
import skytools

from cc.daemon.plugins import CCDaemonPlugin

#
# Base class for pg_logforward plugins
#

class PgLogForwardPlugin (CCDaemonPlugin):
    """ PgLogForward plugin interface """

    LOG_FORMATS = [] # json, netstr, syslog

    log = skytools.getLogger ('d:PgLogForward')

    def probe (self, log_fmt):
        if log_fmt not in self.LOG_FORMATS:
            self.log.debug ("plugin %s does not support %r formatted messages", self.__class__.__name__, log_fmt)
            return False
        return True

    def init (self, log_fmt):
        assert log_fmt in self.LOG_FORMATS
        self.msg_format = log_fmt

    def process (self, msg):
        m = { "json": self.process_json,
            "netstr": self.process_netstr,
            "syslog": self.process_syslog }
        m[self.msg_format](msg)

    def process_json (self, msg):
        raise NotImplementedError

    def process_netstr (self, msg):
        raise NotImplementedError

    def process_syslog (self, msg):
        raise NotImplementedError
