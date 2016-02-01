# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
from settings import RPC_HOST, RPC_PORT


class ElricRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    request_queue_size = 10000

    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True


class ElricRPCClient:
    def __init__(self, context):
        self.context = context

    def call(self, func_name, *args):
        try:
            # TODO: server也要修改, 参考http://hgoldfish.com/blogs/article/50/
            server_uri = 'http://{host}:{port}'.format(host=RPC_HOST, port=RPC_PORT)
            server = xmlrpclib.ServerProxy(server_uri, use_datetime=True)
            return getattr(server, func_name)(*args)
        except Exception as e:
            self.context.log.error(e)
