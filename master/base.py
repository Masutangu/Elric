# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
from core.rpc import ElricRPCServer
import threading
from config import RPC_SERVER_HOST, RPC_SERVER_PORT
import logging
import logging.handlers

master_logger = logging.getLogger(__name__)
master_logger.setLevel(logging.DEBUG)
#handler = logging.handlers.RotatingFileHandler('%s.log' % __name__, maxBytes=10000000, backupCount=5)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
master_logger.addHandler(handler)


class BaseMaster(object):
    __metaclass__ = ABCMeta

    def __init__(self, logger=None):
        self.log = logger or master_logger
        self.rpc_server = self.start_rpc_server()
        self.rpc_server.register_function(self.submit_job, 'submit_job')
        self.rpc_server.register_function(self.update_job, 'update_job')
        self.rpc_server.register_function(self.remove_job, 'remove_job')

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of Master must provide a start() method')

    @abstractmethod
    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        raise NotImplementedError('subclasses of Master must provide a submit_job() method')

    @abstractmethod
    def update_job(self, job_id, job_key, next_run_time, serialized_job):
        raise NotImplementedError('subclasses of Master must provide a update_job() method')

    @abstractmethod
    def remove_job(self, job_id):
        raise NotImplementedError('subclasses of Master must provide a remove_job() method')

    def start_rpc_server(self):
        print 'start rpc server...'
        rpc_server = ElricRPCServer((RPC_SERVER_HOST, RPC_SERVER_PORT))
        thd = threading.Thread(target=rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        return rpc_server