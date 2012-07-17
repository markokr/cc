"""
Example plugins for PgLogForward daemon.
"""

import datetime
import re
import socket
import time

import skytools
from zmq.eventloop.ioloop import PeriodicCallback

import cc.json
from cc.daemon.pg_logforward import pg_elevels_atoi
from cc.daemon.plugins.pg_logforward import PgLogForwardPlugin
from cc.message import is_msg_req_valid
from cc.reqs import DatabaseMessage

# log matching regexes

re_disconnect = r"disconnection: \s+ session \s+ time: \s+ (?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>[\d.]+) \s+ user=.*"
rc_disconnect = re.compile (re_disconnect, re.X | re.M)

re_lock_wait = "process \d+ still waiting for (?P<lockname>\w+) on relation \d+ of database \d+ after [0-9.]+ ms"
rc_lock_wait = re.compile (re_lock_wait)

re_logged_func = r"""
function \s call: \s (?P<func_name>[\w.]+\(\d+\))
\s+ calls=(?P<calls>\d+)
\s+ time=(?P<time>\d+)
\s+ self_time=(?P<self_time>)\d+
"""
rc_logged_func = re.compile (re_logged_func, re.X | re.I | re.S)

re_sql = r"""
duration: \s+ (?P<duration>[0-9.]+) \s+ (?P<unit>[a-z]+) \s+
(statement|execute\s<.*>): \s* (?P<sql> .* (?: [\n][\t] .* )* )
"""
rc_sql = re.compile (re_sql, re.X | re.M)


class LogWatch_ProcessErrors (PgLogForwardPlugin):
    LOG_FORMATS = ['netstr']

    def init (self, log_fmt):
        super(LogWatch_ProcessErrors, self).init(log_fmt)

        self.hostname = socket.gethostname()
        self.error_reset_interval = self.cf.getint ('error_reset_interval', 300)
        self.last_error_time = time.time()

        self.msg_suffix = self.cf.get ('msg-suffix', 'confdb')
        if self.msg_suffix and not is_msg_req_valid (self.msg_suffix):
            self.log.error ("invalid msg-suffix: %s", self.msg_suffix)
            self.msg_suffix = None

    def process_netstr (self, data):
        """
        Check line for error messages, report to logdb as needed.
        Also report that we are OK if sufficient time has elapsed since last error.

        We allow only N errors in interval, after that N is reached it is assumed that
        errors are flooding and FATAL message is logged.  If the error rate goes down we
        start logging details again.
        """

        now = time.time()
        time_passed = now - self.last_error_time

        if data['elevel_text'] == 'LOG':
            if data['funcname'] == 'ProcSleep':
                m = rc_lock_wait.search (data['message'])
                if m: # Raise the priority for lock wait messages
                    data['elevel_text'] = 'WARNING'
                    data['message'] = "%s lock waits" % m.group("lockname")
            elif time_passed >= self.error_reset_interval:
                # Occassionally let ordinary LOG level messages through to clear
                # possible error status for the service.
                data['elevel_text'] = 'INFO'

        # log only known levels
        if data['elevel_text'] not in ['INFO', 'WARNING', 'ERROR', 'FATAL']:
            return

        data['elevel'] = pg_elevels_atoi[data['elevel_text']] # catch up

        if not data['database']:
            data['database'] = 'postgres'

        self.log.trace ('error_msg: %s' % data)

        # post message
        funcargs = [self.hostname, None, 'postgres_monitoring', data['elevel_text'], data['database'],
                "%s %s: %s" % (data['username'], data['remotehost'], data['message'])]
        msg = DatabaseMessage (function = 'log.add', params = cc.json.dumps(funcargs))
        if self.msg_suffix:
            msg.req += '.' + self.msg_suffix
        self.main.ccpublish(msg)

        self.last_error_time = now


class LogWatch_HandleStats (PgLogForwardPlugin):
    LOG_FORMATS = ['netstr']

    def init (self, log_fmt):
        super(LogWatch_HandleStats, self).init(log_fmt)

        # depends on pg_settings.log_function_calls
        self.parse_statements = self.cf.getbool ('parse_statements', True)

        self.msg_suffix = self.cf.get ('msg-suffix', 'confdb')
        if self.msg_suffix and not is_msg_req_valid (self.msg_suffix):
            self.log.error ("invalid msg-suffix: %s", self.msg_suffix)
            self.msg_suffix = None

        self.hostname = socket.gethostname()
        self.stat_queue_name = self.cf.get ('stat_queue_name', '')
        self.max_stat_items = self.cf.get ('max_stat_items', 10000)
        self.stat_dump_interval = self.cf.getint ('stat_interval', 3600)
        self.last_stat_dump = time.time()
        self.client_stats = {}

        self.timer = PeriodicCallback (self.save_stats, self.stat_dump_interval * 1000)
        self.timer.start()

    def process_netstr (self, data):
        """
        Process contents of collected log chunk.
        This might be a SQL statement or a connect/disconnect entry.
        """
        if not self.stat_queue_name:
            return

        if data['remotehost'] == "[local]":
            data['remotehost'] = "127.0.0.1"

        action = None
        action_duration = 0
        statement_duration = 0
        call_count = 0

        if data['message'].startswith ("connection authorized:"):
            action = "connect"
        elif data['message'].startswith ("disconnection"):
            action = "disconnect"
            m = rc_disconnect.match (data['message'])
            if m:
                action_duration = (int(m.group('hours')) * 3600 +
                                   int(m.group('minutes')) * 60 +
                                   float(m.group('seconds'))) * 1000
        elif not self.parse_statements:
            # we have function logging enabled, see if we can use it
            m = rc_logged_func.search (data['message'])
            if m:
                # a logged function call, definitely prefer this to parsing
                action = m.group('func_name')
                action_duration = float(m.group('time')) / 1000
                call_count = int(m.group('calls'))
        if not action:
            # we have to parse function call
            m = rc_sql.search (data['message'])
            if m:
                if self.parse_statements:
                    # attempt to parse the function name and parameters
                    #action = self.get_sql_action (m.group('sql'))
                    call_count = 1
                # count the overall statement duration
                action_duration = float(m.group('duration'))
                statement_duration = action_duration

        self._update_stats (data, action, action_duration, call_count)
        self._update_stats (data, "SQL statements", statement_duration, call_count)

    def _update_stats (self, data, action, duration, call_count):
        if action:
            key = (data['database'], data['username'], data['remotehost'], action)
            cs = self.client_stats.get(key)
            if cs:
                cs.update (duration, call_count)
            elif len(self.client_stats) > self.max_stat_items:
                self.log.error ("Max stat items exceeded: %i", self.max_stat_items)
            else:
                cs = ClientStats (data['database'], data['username'], data['remotehost'], action, duration, call_count)
                self.client_stats[key] = cs

    def save_stats (self):
        """
        Dump client stats to database.  Scheduled to be called periodically.
        """

        # do not send stats if stats is missing or stats queue is missing
        if not self.client_stats or not self.stat_queue_name:
            return

        now = time.time()
        time_passed = now - self.last_stat_dump
        self.log.info ("Sending usage stats to repository [%i]", len(self.client_stats))

        # post role usage
        usage = []
        for client in self.client_stats.values():
            self.log.trace ("client: %s", client)
            usage.append (client.to_dict())

        params = skytools.db_urlencode(dict(
            hostname = self.hostname,
            sample_length = '%d seconds' % time_passed,
            snap_time = datetime.datetime.now().isoformat()))
        confdb_funcargs = ('username=discovery', params, skytools.make_record_array(usage))

        funcargs = [None, self.stat_queue_name, 'dba.set_role_usage',
                skytools.db_urlencode(dict(enumerate(confdb_funcargs)))]

        msg = DatabaseMessage (function = 'pgq.insert_event', params = cc.json.dumps(funcargs))
        if self.msg_suffix:
            msg.req += '.' + self.msg_suffix
        self.main.ccpublish(msg)

        self.client_stats = {}
        self.last_stat_dump = now

    def stop (self):
        self.timer.stop()


class ClientStats:
    """ User activity stats """

    def __init__ (self, database, username, fromaddr, action, duration, call_count):
        self.database = database
        self.username = username
        self.fromaddr = fromaddr
        self.action = action
        self.duration = duration
        self.count = call_count
        self.count_dur5 = 0
        self.count_dur20 = 0

    def key (self):
        return (self.database, self.username, self.fromaddr, self.action)

    def update (self, duration, call_count):
        self.count += call_count
        self.duration += duration
        if duration > 5000:
            self.count_dur5 += 1
        if duration > 20000:
            self.count_dur20 += 1

    def to_dict(self):
        return dict(role_name=self.username, database=self.database,
            ip_address=self.fromaddr, action=self.action, log_count=self.count,
            log_duration=int(self.duration),
            log_count_dur5 = self.count_dur5,
            log_count_dur20 = self.count_dur20)

    def __str__(self):
        return "UserStat: user=%s from=%s db=%s action=%s count=%d duration=%.3f" % \
            (self.username, self.fromaddr, self.database, self.action, self.count, self.duration)
