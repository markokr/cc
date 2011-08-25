#! /usr/bin/env python

"""Run script periodically, send output.
"""

import sys, os, os.path, logging, time

from zmq.eventloop.ioloop import PeriodicCallback, IOLoop
from subprocess import Popen, PIPE, STDOUT
from cc.daemon import CCDaemon
from cc.reqs import InfofileMessage

import skytools

class StrictPeriod(PeriodicCallback):
    """Calculate period before launching callback"""
    def _run(self):
        if not self._running:
            return
        # time new event before running callback
        self.start()
        try:
            self.callback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            logging.error("Error in periodic callback", exc_info=True)


class InfoScript(CCDaemon):
    """Run script, send output.
    """

    def startup(self):
        super(InfoScript, self).startup()

        self.ioloop = IOLoop.instance()
        self.info_script = self.cf.get('info-script')
        self.info_period = self.cf.getfloat('info-period')
        self.info_name = self.cf.get('info-name')

        self.timer = StrictPeriod(self.run_info_script, self.info_period*1000, self.ioloop)
        self.timer.start()
        self.run_info_script()

    def run_info_script(self):
        self.log.info('Running: %s', self.info_script)

        # launch command
        p = Popen(self.info_script, close_fds=True, shell=True,
                  stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        res = p.communicate()[0]
        if p.returncode != 0:
            self.log.error("Info script '%s' run failure (errcode=%d): %s",
                           self.info_script, p.returncode, repr(res))
            return

        msg = InfofileMessage(req = 'pub.infofile',
                              filename = self.info_name,
                              hostname = self.hostname,
                              mtime = time.time(),
                              data = res)
        self.ccpublish(msg)

    def work(self):
        #self.connect_cc()
        self.ioloop.start()
        return 1

if __name__ == '__main__':
    script = InfoScript('infoscript', sys.argv[1:])
    script.start()
