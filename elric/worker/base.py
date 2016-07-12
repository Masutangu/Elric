# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
import logging

from elric.core.log import init_logging_config


class BaseWorker(object):
    __metaclass__ = ABCMeta
    name = None

    def __init__(self, name, logger_name):
        init_logging_config()
        self.log = logging.getLogger(logger_name)
        self.name = name
        if not getattr(self, 'name', None):
            raise ValueError("%s must have a name" % type(self).__name__)

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of BaseWorker must provide a start() method')

    @abstractmethod
    def stop(self):
        raise NotImplementedError('subclasses of BaseWorker must provide a stop() method')





