#! /usr/bin/env python

"""
Handles web-requests from mongrel2.
"""

import sys
import zmq
import json
import hots.script
from hots.webhandler import AjaxConnection
import skytools
import uuid

class Session:
    def __init__(self, session_id, username):
        self.session_id = session_id
        self.username = username
        self.poll_req = None

    def set_poller(self, req):
        self.poll_req = req

    def get_poller(self):
        return self.poll_req

class HotsWeb(hots.script.HotsScript):

    def startup(self):
        hots.script.HotsScript.startup(self)

        sender_id = self.cf.get('sender_uuid')
        wsub = self.get_socket('remote-pull-http')
        wpub = self.get_socket('remote-pub-http')
        self.wconn = AjaxConnection(sender_id, wsub, wpub)

        self.s_confdb = self.get_socket('remote-req-confdb')
        self.s_logsrv = self.get_socket('remote-sub-logsrv')

        # temp hack: subscribe to all
        self.s_logsrv.setsockopt(zmq.SUBSCRIBE, '')

        self.poller = zmq.Poller()
        self.poller.register(self.s_confdb, zmq.POLLIN)
        self.poller.register(self.s_logsrv, zmq.POLLIN)
        self.poller.register(self.wconn.reqs, zmq.POLLIN)

        self.session_map = {}

    def work(self):
        smap = dict(self.poller.poll(1*60*1000))
        if smap.get(self.wconn.reqs) == zmq.POLLIN:
            self.handle_web()
        if smap.get(self.s_confdb) == zmq.POLLIN:
            self.handle_confdb()
        if smap.get(self.s_logsrv) == zmq.POLLIN:
            self.handle_logsrv()
        return 1

    def handle_web(self):
        req = self.wconn.recv()
        nicelist = ('QUERY', 'URI', 'METHOD')
        nicehdrs = {}
        for k in nicelist:
            if k in req.headers:
                nicehdrs[k] = req.headers[k]

        if req.is_disconnect():
            self.log.info('REPLY: disconnect')
            return
        elif req.headers.get("killme", False):
            self.log.info('REPLY: killme')
            self.wconn.reply_http(req, '')
            return

        qry = req.headers.get('QUERY', '')
        qry = skytools.db_urldecode(qry)
        reqname = qry.get('req', '')
        username = qry.get('user', '')
        session_id = qry.get('session_id', '')
        resp = { 'req': reqname, 'msg': 'Msg unset' }

        # process login before session auth
        if reqname == 'login':
            session_id = str(uuid.uuid4())
            self.session_map[session_id] = Session(session_id, username)
            resp['session_id'] = session_id
            resp['msg'] = 'New session: ' + session_id
            self.log.info('REPLY: %s' % json.dumps(resp))
            self.wconn.reply_ajax(req, resp)
            return

        if not session_id or session_id not in self.session_map:
            resp = { 'req': reqname, 'msg': 'ERROR: unknown session' }
            self.log.info('REPLY: %s' % json.dumps(resp))
            self.wconn.reply_ajax(req, resp)
            return

        sess = self.session_map[session_id]
        if reqname == 'poll':
            # do not respond to this msg
            self.log.info('poll req stored')
            sess.set_poller(req)
            return

        if reqname == 'start':
            self.log.info('sending req to confdb')
            msg = {'req': reqname}
            self.send_confdb(req, json.dumps(msg))
            return
        else:
            resp['msg'] = 'Unknown request'
        self.log.info('REPLY: %s' % json.dumps(resp))
        self.wconn.reply_ajax(req, resp)

    def handle_logsrv(self):
        msg = self.s_logsrv.recv()
        self.log.info('LOGSRV: ' + msg)
        for sess in self.session_map.values():
            req = sess.get_poller()
            resp = {'req': 'poll', 'msg': 'Logsrv: '+str(msg) }
            self.wconn.reply_ajax(req, resp)
            # drop old req?

    confdb_waiter = None
    confdb_queue = []
    def handle_confdb(self):
        jmsg = self.s_confdb.recv()
        self.log.info('CONFDB: %s' % jmsg)
        msg = json.loads(jmsg)
        req = self.confdb_waiter
        self.wconn.reply_ajax(req, msg)
        self.confdb_waiter = None

        if self.confdb_queue:
            req, msg = self.confdb_queue.pop(0)
            self.send_confdb(req, msg)

    def send_confdb(self, req, json):
        if self.confdb_waiter:
            self.confdb_queue.append( (req, json) )
        else:
            self.s_confdb.send(json)
            self.confdb_waiter = req

if __name__ == '__main__':
    s = HotsWeb('hots-websrv', sys.argv[1:])
    s.start()

