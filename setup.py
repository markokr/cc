#! /usr/bin/env python

from distutils.core import setup

ln = open('debian/changelog', 'r').readline()
ver = ln[ ln.index('(')+1 : ln.index(')') ]

setup(
    name = "cc",
    version = ver,
    maintainer = "Marko Kreen",
    maintainer_email = "marko.kreen@skype.net",
    packages = ['cc', 'cc.handler', 'cc.daemon'],
    scripts = [
        'bin/ccserver.py',
    ],
    data_files = [
        ('share/doc/cc', ['conf/cclocal.ini', 'conf/ccserver.ini']),
    ],
)

