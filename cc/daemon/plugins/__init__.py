
#
# Base class for plugins
#

class CCDaemonPlugin (object):
    """ Base CC daemon plugin interface """

    log = None

    def __init__ (self, pname, pcf, main):
        super(CCDaemonPlugin, self).__init__()
        self.name = pname
        self.cf = pcf
        self.main = main

    def probe (self):
        raise NotImplementedError

    def init (self):
        pass

    def stop (self):
        pass
