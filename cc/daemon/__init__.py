import sys
import types

import skytools

from cc.job import CCJob
from cc.daemon.plugins import CCDaemonPlugin

#
# Base class for daemons
#

class CCDaemon (CCJob):
    log = skytools.getLogger ('d:CCDaemon')

    def find_plugins (self, mod_name, probe_func = None):
        """ plugin lookup helper """
        p = []
        __import__ (mod_name)
        m = sys.modules [mod_name]
        for an in dir (m):
            av = getattr (m, an)
            if (isinstance (av, types.TypeType) and
                issubclass (av, CCDaemonPlugin) and
                av.__module__ == m.__name__):
                if not probe_func or probe_func (av):
                    p += [av]
                else:
                    self.log.debug ("plugin %s probing negative", an)
        if not p:
            self.log.info ("no suitable plugins found in %s", mod_name)
        return p

    def load_plugins (self, *args, **kwargs):
        """ Look for suitable plugins, probe them, load them.
        """
        self.plugins = []
        for palias in self.cf.getlist ('plugins'):
            pcf = self.cf.clone (palias)
            mod = pcf.get ('module')
            for cls in self.find_plugins (mod):
                pin = cls (palias, pcf, self)
                if pin.probe (*args, **kwargs):
                    self.plugins += [pin]
                else:
                    self.log.debug ("plugin %s probing negative", pin.__class__.__name__)
        if self.plugins:
            self.log.info ("Loaded plugins: %s", [p.__class__.__name__ for p in self.plugins])
        else:
            self.log.warn ("No plugins loaded!")
