#! /usr/bin/env python

"""CC handler classes.

Called in CC main loop, from single thread.
They need to do something with the message and *fast*:

- Push into remote ZMQ socket
- Push into local ZMQ socket to local worker processes/threads
- Write into file?

- No time-consuming processing.

It would be preferable to reduce everything to write to socket.

"""

import sys, time, json, zmq, os, os.path, subprocess

from zmq.eventloop.ioloop import PeriodicCallback
from cc.message import CCMessage
from cc.stream import CCStream

import skytools


__all__ = ['cc_handler_lookup']

class BaseHandler(object):
    """Store handler config."""
    def __init__(self, hname, hcf, ccscript):
        self.hname = hname
        self.cf = hcf
        self.cclocal = ccscript.local
        self.zctx = ccscript.zctx
        self.ioloop = ccscript.ioloop
        self.log = ccscript.log

    def handle_msg(self, rmsg):
        pass

#
# message proxy
#

class ProxyHandler(BaseHandler):
    """Simply proxies further"""
    def __init__(self, hname, hcf, ccscript):
        super(ProxyHandler, self).__init__(hname, hcf, ccscript)

        s = self.make_socket()
        s.setsockopt(zmq.LINGER, 500)
        self.stream = CCStream(s, ccscript.ioloop)
        self.stream.on_recv(self.on_recv)

        self.launch_workers()

        self.stat_increase = ccscript.stat_increase

    def launch_workers(self):
        pass

    def make_socket(self):
        zurl = self.cf.get('remote-cc')
        s = self.zctx.socket(zmq.XREQ)
        s.setsockopt(zmq.LINGER, 500)
        s.connect(zurl)
        return s

    def on_recv(self, zmsg):
        """Got message from remote CC, send to client."""
        try:
            self.log.debug('ProxyHandler.handler.on_recv')
            cmsg = CCMessage(zmsg)
            self.stat_increase('count')
            self.stat_increase('bytes', cmsg.get_size())
            self.cclocal.send_multipart(zmsg)
        except:
            self.log.exception('ProxyHandler.on_recv crashed, dropping msg')

    def handle_msg(self, cmsg):
        """Got message from client, send to remote CC"""
        self.stream.send_cmsg(cmsg)

#
# db proxy
#

def db_worker(zctx, worker_url, connstr):
    """Worker thread, can do blocking calls."""
    s = zctx.socket(zmq.REP)
    s.connect(worker_url)
    while 1:
        cmsg = s.recv_cmsg()
        s.send_multipart(['fooz', '{fooz}'])

class DBHandler(ProxyHandler):
    """Send request to workers."""
    def make_socket(self):
        baseurl = 'tcp://127.0.0.1'
        s = self.zctx.socket(zmq.XREQ)
        port = s.bind_to_random_port('tcp://127.0.0.1')
        self.worker_url = "%s:%d" % (baseurl, port)
        return s

    def launch_workers(self):
        nworkers = 10
        wargs = (self.zctx, self.worker_url, self.cf.get('db'))
        for i in range(nworkers):
            threading.Thread(target = db_worker, args = wargs)

#
# task router
#

class HostRoute(object):
    """ZMQ route for one host."""

    __slots__ = ('host', 'route', 'create_time')

    def __init__(self, host, route):
        assert isinstance(route, list)
        self.host = host
        self.route = route
        self.create_time = time.time()

class TaskRouter(BaseHandler):
    """Keep track of host routes.
    
    Clean old ones.
    """
    def __init__(self, *args):
        super(TaskRouter, self).__init__(*args)
        self.route_map = {}
        
        # 1 hr?
        self.route_lifetime = 1 * 60 * 60
        self.maint_period = 1 * 60

        self.timer = PeriodicCallback(self.do_maint, self.maint_period*1000, self.ioloop)
        self.timer.start()

    def handle_msg(self, cmsg):
        req = cmsg.get_dest()
        route = cmsg.get_route()
        data = cmsg.get_payload()

        cmd = data['req']
        host = data['host']

        if req == 'req.task.register':
            self.register_host(host, route)
        elif req == 'req.task.send':
            self.send_host(host, cmsg)
        else:
            self.log.warning('TaskRouter: unknown msg: %s', req)

    def do_maint(self):
        """Drop old routes"""
        self.log.info('TaskRouter.do_maint')
        now = time.time()
        zombies = []
        for hr in self.route_map.itervalues():
            if now - hr.create_time > self.route_lifetime:
                zombies.append(hr)
        for hr in zombies:
            self.log.info('TaskRouter: deleting route for %s', hr.host)
            del self.route_map[hr.host]

    def send_host(self, host, cmsg):
        """Send message for task executor on host"""

        if host not in self.route_map:
            self.log.info('TaskRouter: cannot route to %s', host)
            return

        # find ZMQ route
        hr = self.route_map[host]

        # re-construct message
        msg = cmsg.get_non_route()
        zmsg = hr.route + [''] + msg

        # send the message
        self.log.info('TaskRouter: sending task to %s', host)
        self.cclocal.send_multipart(zmsg)

        zans = cmsg.get_route() + [''] + ['OK']
        self.cclocal.send_multipart(zans)

    def register_host(self, host, route):
        """Remember ZMQ route for host"""
        self.log.info('register_host(%s, %s)', repr(host), repr(route))
        hr = HostRoute(host, route)
        self.route_map[hr.host] = hr

        zans = route + [''] + ['OK']
        self.cclocal.send_multipart(zans)

#
# infofile writer
#

def write_atomic(fn, data):
    """Write with rename."""
    fn2 = fn + '.new'
    f = open(fn2, 'w')
    f.write(data)
    f.close()
    os.rename(fn2, fn)

class InfoWriter(BaseHandler):
    """Simply writes to files."""
    def __init__(self, hname, hcf, ccscript):
        super(InfoWriter, self).__init__(hname, hcf, ccscript)

        self.dstdir = hcf.getfile('dstdir')
        self.make_subdirs = hcf.getint('host-subdirs', 0)

    def handle_msg(self, cmsg):
        """Got message from client, send to remote CC"""


        data = cmsg.get_payload()
        mtime = data['mtime']
        host = data['hostname']
        fn = os.path.basename(data['filename'])
        # sanitize
        host = host.replace('/', '_')

        # decide destination file
        if self.make_subdirs:
            subdir = os.path.join(self.dstdir, host)
            dstfn = os.path.join(subdir, fn)
            if not os.isdir(subdir):
                os.mkdir(subdir)
        else:
            dstfn = os.path.join(self.dstdir, '%s--%s' % (host, fn))

        # check if file exist and is older
        try:
            st = os.stat(dstfn)
            if st.st_mtime == mtime:
                self.log.info('InfoWriter.handle_msg: %s mtime matches, skipping', fn2)
            elif st.st_mtime > mtime:
                self.log.info('InfoWriter.handle_msg: %s mtime newer, skipping', fn2)
        except OSError:
            pass

        # write file, apply original mtime
        self.log.info('InfoWriter.handle_msg: writing data to %s', dstfn)
        write_atomic(dstfn, data['data'])
        os.utime(dstfn, (mtime, mtime))

#
# JobMgr
#

class JobState:
    def __init__(self, jname, jcf):
        self.jname = jname
        self.jcf = jcf

class JobMgr(BaseHandler):
    """Provide config to local daemons / tasks."""

    def __init__(self, hname, hcf, ccscript):
        super(JobMgr, self).__init__(hname, hcf, ccscript)

        self.jobs = {}
        for dname in self.cf.getlist('daemons'):
            self.add_job(dname)

    def add_job(self, jname):
        jcf = skytools.Config(jname, self.cf.filename, ignore_defs = True)
        self.jobs[jname] = JobState(jname, jcf)
        
        # unsure about the best way to specify target
        mod = jcf.get('module', '')
        script = jcf.get('module', '')
        cls = jcf.get('class', '')
        if mod:
            cmd = ['python', '-m', mod, '-d', '--ccdaemon', jname]
        elif script:
            cmd = [script, '-d', '--ccdaemon', jname]
        else:
            raise UsageError('dunno how to launch class')

        self.log.info('Launching %s: %s', jname, cmd)
        p = subprocess.Popen(cmd, close_fds=True,
                                stdin = open(os.devnull, 'rb'),
                                stdout = open(os.devnull, 'wb'),
                                stderr = open(os.devnull, 'wb'))

    def handle_msg(self, cmsg):
        """Got message from client, send to remote CC"""

        self.log.info('JobMgr req: %s', cmsg)
        data = cmsg.get_payload()

        res = {'req': data['req']}
        if data['req'] == 'job.config':
            # fill defaults
            job = self.jobs[data['job_name']]
            cf = {
                    'job_name': data['job_name'],
                    'pidfiledir': self.cf.getfile('pidfiledir', '~/pid'),
                    'pidfile': '%(pidfiledir)s/%(job_name)s.pid',
            }

            # move config from cc-s .ini file
            for o in job.jcf.options():
                cf[o] = job.jcf.get(o)
            res['config'] = cf
        else:
            res['msg'] = 'Unsupported req'
        ans = cmsg.make_reply(res)
        self.cclocal.send_cmsg(ans)
        self.log.info('JobMgr answer: %s', ans)

#
# local logger
#

class LocalLogger(BaseHandler):
    """Log as local log msg."""
    def handle_msg(self, cmsg):
        data = cmsg.get_payload()
        self.log.info('[%s] %s %s', data['job_name'], data['level'], data['msg'])

#
# name->class lookup
#

_handler_lookup = {
    'proxy': ProxyHandler,
    'dbhandler': DBHandler,
    'taskrouter': TaskRouter,
    'infowriter': InfoWriter,
    'jobmgr': JobMgr,
    'locallogger': LocalLogger,
}

def cc_handler_lookup(hname):
    return _handler_lookup[hname]

