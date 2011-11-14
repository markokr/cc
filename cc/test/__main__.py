"""Hopefully this will work on installed CC too."""

from cc.test import test_basic, test_infofile, test_task
modlist = ['test_basic', 'test_infofile', 'test_task']

import unittest
unittest.main(argv = ['cc.test', '-v'] + modlist)

