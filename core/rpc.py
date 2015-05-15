# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib


class ElricRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True


def rpc_client_call(host, port, func_name, *args):
    # TODO: server也要修改, 参考http://hgoldfish.com/blogs/article/50/
    server_uri = 'http://{host}:{port}'.format(host=host, port=port)
    server = xmlrpclib.ServerProxy(server_uri, use_datetime=True)
    return getattr(server, func_name)(*args)
