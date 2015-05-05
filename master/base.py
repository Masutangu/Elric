__author__ = 'Masutangu'
from abc import ABCMeta, abstractmethod
from core.rpc import ElricRPCServer
import threading
from config import RPC_SERVER_HOST, RPC_SERVER_PORT


class ElricMaster(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.start_rpc_server()
        self.rpc_server.register_function(self.submit_job, 'submit_job')
        self.rpc_server.register_function(self.cancel_job, 'cancel_job')


    @abstractmethod
    def run(self):
        raise NotImplementedError('subclasses of Master must provide a run() method')


    @abstractmethod
    def submit_job(self, key, job):
        raise NotImplementedError('subclasses of Master must provide a submit_job() method')


    @abstractmethod
    def cancel_job(self, key, job):
        raise NotImplementedError('subclasses of Master must provide a cancel_job() method')


    def start_rpc_server(self):
        print 'start rpc server...'
        rpc_server = ElricRPCServer((RPC_SERVER_HOST, RPC_SERVER_PORT))
        thd = threading.Thread(target=rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        self.rpc_server = rpc_server