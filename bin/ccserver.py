#! /usr/bin/env python

import sys
from cc.server import CCServer

CCServer('ccserver', sys.argv[1:]).start()

