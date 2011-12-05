
import skytools
from cc.job import CCJob

#
# Base class for daemons
#

class CCDaemon(CCJob):
    log = skytools.getLogger('d:CCDaemon')
