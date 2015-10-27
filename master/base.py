# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
from core.rpc import ElricRPCServer
import threading
from settings import RPC_HOST, RPC_PORT
import logging
import logging.handlers


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class BaseMaster(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.log = setup_logger()
        self.rpc_server = self.start_rpc_server(RPC_HOST, RPC_PORT)
        self.rpc_server.register_function(self.submit_job, 'submit_job')
        self.rpc_server.register_function(self.update_job, 'update_job')
        self.rpc_server.register_function(self.remove_job, 'remove_job')

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of BaseMaster must provide a start() method')

    @abstractmethod
    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        raise NotImplementedError('subclasses of BaseMaster must provide a submit_job() method')

    @abstractmethod
    def update_job(self, job_id, job_key, next_run_time, serialized_job):
        raise NotImplementedError('subclasses of BaseMaster must provide a update_job() method')

    @abstractmethod
    def remove_job(self, job_id):
        raise NotImplementedError('subclasses of BaseMaster must provide a remove_job() method')

    def start_rpc_server(self, rpc_host, rpc_port):
        """
            Start rpc server

            :type rpc_host: String
            :type rpc_port: Integer
        """
        self.log.debug('start rpc server...')
        rpc_server = ElricRPCServer((rpc_host, rpc_port))
        thd = threading.Thread(target=rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        return rpc_server