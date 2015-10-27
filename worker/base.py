# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
import logging
import logging.handlers


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    #handler = logging.handlers.RotatingFileHandler('%s.log' % __name__, maxBytes=10000000, backupCount=5)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class BaseWorker(object):
    __metaclass__ = ABCMeta
    name = None

    def __init__(self, name):
        self.name = name
        if not getattr(self, 'name', None):
            raise ValueError("%s must have a name" % type(self).__name__)
        self.log = setup_logger()

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of BaseWorker must provide a start() method')

    @abstractmethod
    def stop(self):
        raise NotImplementedError('subclasses of BaseWorker must provide a stop() method')





