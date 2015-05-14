# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
from config import RPC_CLIENT_URI
import logging
import logging.handlers

import xmlrpclib

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
        self.rpc_client = self.init_rpc_client()

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of Master must provide a run() method')

    def init_rpc_client(self):
        self.log.debug('init rpc client')
        # TODO: server也要修改, 参考http://hgoldfish.com/blogs/article/50/
        return xmlrpclib.ServerProxy(RPC_CLIENT_URI, use_datetime=True)

    @abstractmethod
    def stop(self):
        raise NotImplementedError


