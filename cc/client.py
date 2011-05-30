
import json
import os.path

def make_stats_msg(sdict):
    mdict = {
        'req': 'pub.stats',
        'stats': sdict
    }
    return [mdict['req'], json.dumps(mdict)]

def make_infofile_msg(fn):
    mdict = {
        'req': 'pub.infofile',
        'infofile': os.path.basename(fn)
        'body': open(fn, 'r').read()
    }
    return [mdict['req'], json.dumps(mdict)]

def make_infofile_msg(fn):
    mdict = {
        'req': 'pub.infofile',
        'infofile': os.path.basename(fn)
        'body': open(fn, 'r').read()
    }
    return [mdict['req'], json.dumps(mdict)]

class Client:
    def __init__(self, url):
        self.sock = zmq.
    def send_stats(self, sdict):
        pass
    def send_infofile(self, fn):
        pass
    def send_state(self, fn):
        pass
    def send_log(self, fn):
        pass
    def query(self, qry):
    def query_async(self, qry):
        pass
