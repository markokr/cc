import os, os.path

import cc.util

from cc.handler import CCHandler

__all__ = ['InfoWriter']

CC_HANDLER = 'InfoWriter'

#
# infofile writer
#

comp_ext = {
    'gzip': '.gz',
    'bzip2': '.bz2',
    }

class InfoWriter(CCHandler):
    """Simply writes to files."""

    CC_ROLES = ['remote']

    def __init__(self, hname, hcf, ccscript):
        super(InfoWriter, self).__init__(hname, hcf, ccscript)

        self.dstdir = hcf.getfile('dstdir')
        self.host_subdirs = hcf.getboolean('host-subdirs', 0)
        self.bakext = hcf.get('bakext', '')
        self.write_compressed = hcf.get ('write-compressed', '')
        assert self.write_compressed in [None, '', 'no', 'keep', 'yes']
        if self.write_compressed == 'yes':
            self.compression = self.cf.get ('compression', '')
            if self.compression not in ('gzip', 'bzip2'):
                self.log.error ("unsupported compression: %s", self.compression)
            self.compression_level = self.cf.getint ('compression-level', '')

    def handle_msg(self, cmsg):
        """Got message from client, send to remote CC"""

        data = cmsg.get_payload(self.xtx)
        if not data: return

        mtime = data['mtime']
        host = data['hostname']
        fn = os.path.basename(data['filename'])
        # sanitize
        host = host.replace('/', '_')

        # add file ext if needed
        if self.write_compressed == 'keep':
            if data['comp'] not in [None, '', 'none']:
                fn += comp_ext[data['comp']]
        elif self.write_compressed == 'yes':
            fn += comp_ext[self.compression]

        # decide destination file
        if self.host_subdirs:
            subdir = os.path.join(self.dstdir, host)
            dstfn = os.path.join(subdir, fn)
            if not os.path.isdir(subdir):
                os.mkdir(subdir)
        else:
            dstfn = os.path.join(self.dstdir, '%s--%s' % (host, fn))

        # check if file exists and is older
        try:
            st = os.stat(dstfn)
            if st.st_mtime == mtime:
                self.log.info('InfoWriter.handle_msg: %s mtime matches, skipping', dstfn)
                return
            elif st.st_mtime > mtime:
                self.log.info('InfoWriter.handle_msg: %s mtime newer, skipping', dstfn)
                return
        except OSError:
            pass

        raw = data['data'].decode('base64')
        if self.write_compressed in [None, '', 'no', 'keep']:
            if self.write_compressed != 'keep':
                body = cc.util.decompress (raw, data['comp'])
                self.log.debug ("InfoWriter.handle_msg: decompressed from %i to %i", len(raw), len(body))
            else:
                body = raw
        elif self.write_compressed == 'yes':
            if (data['comp'] != self.compression):
                deco = cc.util.decompress (raw, data['comp'])
                body = cc.util.compress (deco, self.compression, {'level': self.compression_level})
                self.log.debug ("InfoWriter.handle_msg: compressed from %i to %i", len(raw), len(body))
            else:
                body = raw

        # write file, apply original mtime
        self.log.debug('InfoWriter.handle_msg: writing data to %s', dstfn)
        cc.util.write_atomic (dstfn, body, bakext = self.bakext)
        os.utime(dstfn, (mtime, mtime))
