__author__ = 'Masutangu'
from abc import ABCMeta, abstractmethod
from config import RPC_CLIENT_URI
import logging
import logging.handlers

import xmlrpclib


worker_logger = logging.getLogger('worker')
worker_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler('worker.log', maxBytes=10000000, backupCount=5)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
worker_logger.addHandler(handler)


class BaseWorker(object):
    __metaclass__ = ABCMeta

    def __init__(self, logger=None):
        self.log = logger or worker_logger
        self.init_rpc_client()

    @abstractmethod
    def run(self):
        raise NotImplementedError('subclasses of Master must provide a run() method')

    def init_rpc_client(self):
        print 'init rpc client'
        self.rpc_client = xmlrpclib.ServerProxy(RPC_CLIENT_URI)

    @abstractmethod
    def stop(self):
        raise NotImplementedError


