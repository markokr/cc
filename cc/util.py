"""Low-level utilities"""

import os
import fcntl
import errno

__all__ = ['write_atomic', 'set_nonblocking', 'set_cloexec']

def write_atomic(fn, data, bakext=None):
    """Write with rename."""

    # link old data to bak file
    if bakext:
        try:
            os.remove(fn + bakext)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        try:
            os.link(fn, fn + bakext)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise

    # write new data to tmp file
    fn2 = '.' + fn + '.new'
    f = open(fn2, 'w')
    f.write(data)
    f.close()

    # atomically replace file
    os.rename(fn2, fn)

def set_nonblocking(fd, onoff):
    """Toggle the O_NONBLOCK flag.
    If onoff==None then return current setting.
    """
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if onoff is None:
        return (flags & os.O_NONBLOCK) > 0
    if onoff:
        flags |= os.O_NONBLOCK
    else:
        flags &= ~os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)

def set_cloexec(fd, onoff):
    """Toggle the FD_CLOEXEC flag.
    If onoff==None then return current setting.
    """
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    if onoff is None:
        return (flags & fcntl.FD_CLOEXEC) > 0
    if onoff:
        flags |= fcntl.FD_CLOEXEC
    else:
        flags &= ~fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)

