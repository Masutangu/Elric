# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
import logging
import logging.handlers
from config import RPC_SERVER_HOST, RPC_SERVER_PORT

worker_logger = logging.getLogger(__name__)

worker_logger.setLevel(logging.DEBUG)
#handler = logging.handlers.RotatingFileHandler('%s.log' % __name__, maxBytes=10000000, backupCount=5)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
worker_logger.addHandler(handler)


class BaseWorker(object):
    __metaclass__ = ABCMeta
    name = None

    def __init__(self, name, logger=None):
        self.name = name
        if not getattr(self, 'name', None):
            raise ValueError("%s must have a name" % type(self).__name__)
        self.log = logger or worker_logger
        self.rpc_host = RPC_SERVER_HOST
        self.rpc_port = RPC_SERVER_PORT

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of Master must provide a run() method')


    @abstractmethod
    def stop(self):
        raise NotImplementedError


