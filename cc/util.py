"""Low-level utilities"""

import base64
import bz2
import errno
import fcntl
import gzip
import os
import StringIO

__all__ = ['write_atomic', 'set_nonblocking', 'set_cloexec']

def write_atomic (fn, data, bakext = None, mode = 'b'):
    """Write [text] file with rename."""

    if mode not in ['', 'b']:
        raise ValueError ("unsupported fopen mode")

    # link old data to bak file
    if bakext:
        if bakext.find('/') >= 0:
            raise Exception ("invalid bakext")
        fnb = fn + bakext
        try:
            os.unlink(fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        try:
            os.link(fn, fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise

    # write new data to tmp file
    fn2 = fn + '.new'
    f = open(fn2, 'w' + mode)
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


def compress (buffer, method, options = {}):
    """ Compress data using given algorithm (compression method) """

    if method in [None, '', 'none']:
        data = buffer
    elif method == 'gzip':
        cl = options.get ('level', 6) or 6
        cs = StringIO.StringIO()
        gz = gzip.GzipFile (fileobj = cs, mode = 'wb', compresslevel = cl)
        gz.write (buffer)
        gz.close()
        data = base64.b64encode (cs.getvalue())
        cs.close()
    elif method == 'bzip2':
        cl = options.get ('level', 3) or 3
        data = base64.b64encode (bz2.compress (buffer, compresslevel = cl))
    else:
        raise NotImplementedError ("unknown compression: %s" % method)
    return data


def decompress (buffer, method, options = {}):
    """ Decompress data using given algorithm (method) """

    if method in [None, '', 'none']:
        data = buffer
    elif method == 'gzip':
        cs = StringIO.StringIO (base64.b64decode (buffer))
        gz = gzip.GzipFile (fileobj = cs, mode = 'rb')
        data = gz.read()
        gz.close()
        cs.close()
    elif method == 'bzip2':
        data = bz2.decompress (base64.b64decode (buffer))
    else:
        raise NotImplementedError ("unknown compression: %s" % method)
    return data
