
import os, subprocess

from cc.handler import CCHandler

import skytools

__all__ = ['JobMgr']

CC_HANDLER = 'JobMgr'

#
# JobMgr
#

class JobState:
    def __init__(self, jname, jcf):
        self.jname = jname
        self.jcf = jcf

class JobMgr(CCHandler):
    """Provide config to local daemons / tasks."""

    def __init__(self, hname, hcf, ccscript):
        super(JobMgr, self).__init__(hname, hcf, ccscript)

        self.local_url = ccscript.local_url

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
        args = ['-d', '--cc', self.local_url, '--ccdaemon', jname]
        if mod:
            cmd = ['python', '-m', mod] + args
        elif script:
            cmd = [script] + args
        else:
            raise skytools.UsageError('dunno how to launch class')

        self.log.info('Launching %s: %s', jname, cmd)
        p = subprocess.Popen(cmd, close_fds=True,
                                stdin = open(os.devnull, 'rb'),
                                stdout = subprocess.PIPE,
                                stderr = subprocess.STDOUT)
        outbuf = p.communicate()[0]
        if p.returncode != 0:
            self.log.error('Daemon launch failed (%s, exitcode=%d): %s', jname, p.returncode, outbuf.strip())
        elif outbuf:
            self.log.warning('Noisy daemon startup: [%s], %s', jname, repr(outbuf.strip()))
        else:
            self.log.info('Daemon launch successful: %s', jname)

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

