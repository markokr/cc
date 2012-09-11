"""Low-level utilities"""

import bz2
import errno
import gzip
import os
import re

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

__all__ = ['write_atomic', 'compress', 'decompress', 'hsize_to_bytes']


def write_atomic (fn, data, bakext = None, mode = 'b'):
    """Write [text] file with rename."""

    if mode not in ['', 'b']:
        raise ValueError ("unsupported fopen mode")

    # write new data to tmp file
    fn2 = fn + '.new'
    f = open(fn2, 'w' + mode)
    f.write(data)
    f.close()

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

    # atomically replace file
    os.rename(fn2, fn)


def compress (buffer, method, options = {}):
    """ Compress data using given algorithm (compression method) """

    if method in [None, '', 'none']:
        data = buffer
    elif method == 'gzip':
        cl = options.get ('level', 6) or 6
        cs = StringIO()
        gz = gzip.GzipFile (fileobj = cs, mode = 'wb', compresslevel = cl)
        gz.write (buffer)
        gz.close()
        data = cs.getvalue()
        cs.close()
    elif method == 'bzip2':
        cl = options.get ('level', 3) or 3
        data = bz2.compress (buffer, compresslevel = cl)
    else:
        raise NotImplementedError ("unknown compression: %s" % method)
    return data


def decompress (buffer, method, options = {}):
    """ Decompress data using given algorithm (method) """

    if method in [None, '', 'none']:
        data = buffer
    elif method == 'gzip':
        cs = StringIO (buffer)
        gz = gzip.GzipFile (fileobj = cs, mode = 'rb')
        data = gz.read()
        gz.close()
        cs.close()
    elif method == 'bzip2':
        data = bz2.decompress (buffer)
    else:
        raise NotImplementedError ("unknown compression: %s" % method)
    return data


def hsize_to_bytes (input):
    """ Convert sizes from human format to bytes (string to integer) """

    assert isinstance (input, str)
    m = re.match (r"^([0-9]+) *([KMGTPEZY]?)B?$", input.strip(), re.IGNORECASE)
    if not m: raise ValueError ("cannot parse: %s" % input)
    units = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    bytes = int(m.group(1)) * 1024 ** units.index(m.group(2).upper())
    return bytes


stat_dict = {}

def stat_put (key, value):
    """ Set a stat value. """
    global stat_dict
    stat_dict[key] = value

def stat_inc (key, increase = 1):
    """ Increase a stat value. """
    global stat_dict
    try:
        stat_dict[key] += increase
    except KeyError:
        stat_dict[key] = increase

def reset_stats ():
    global stat_dict
    s = stat_dict.copy()
    stat_dict = {}
    return s
