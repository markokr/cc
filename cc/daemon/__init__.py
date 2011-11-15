
import logging
from cc.job import CCJob

#
# Base class for daemons
#

class CCDaemon(CCJob):
    log = logging.getLogger('d:CCDaemon')

