__author__ = 'Masutangu'
from abc import ABCMeta, abstractmethod
from config import RPC_CLIENT_URI

import xmlrpclib


class ElricWorker(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.init_rpc_client()


    @abstractmethod
    def run(self):
        raise NotImplementedError('subclasses of Master must provide a run() method')


    def init_rpc_client(self):
        print 'init rpc client'
        self.rpc_client = xmlrpclib.ServerProxy(RPC_CLIENT_URI)
