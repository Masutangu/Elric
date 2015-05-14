# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer


class ElricRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True