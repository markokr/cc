#! /usr/bin/env python

import sys
from cc.taskrunner import TaskRunner

TaskRunner('cctaskrunner', sys.argv[1:]).start()

