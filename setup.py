#! /usr/bin/env python

from distutils.core import setup

setup(
    name = "cc",
    version = '0.1',
    maintainer = "Marko Kreen",
    maintainer_email = "marko.kreen@skype.net",
    packages = ['cc'],
    scripts = [
        'bin/ccserver.py',
    ],
)

