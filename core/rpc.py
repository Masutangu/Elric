__author__ = 'moxu'

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

class ElricRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True