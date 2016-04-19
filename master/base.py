# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod
import threading
import logging
from core.log import init_logging_config


class BaseMaster(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        init_logging_config()
        self.log = logging.getLogger('elric.master')
        # TODO: avoid hard code
        self.func_map = {
            '__elric_submit_channel__': self.submit_job,
            '__elric_remove_channel__': self.remove_job,
            '__elric_finish_channel__': self.finish_job
        }

    @abstractmethod
    def start(self):
        raise NotImplementedError('subclasses of BaseMaster must provide a start() method')

    @abstractmethod
    def submit_job(self, job):
        raise NotImplementedError('subclasses of BaseMaster must provide a submit_job() method')

    @abstractmethod
    def remove_job(self, job):
        raise NotImplementedError('subclasses of BaseMaster must provide a remove_job() method')

    @abstractmethod
    def finish_job(self, job):
        raise NotImplementedError('subclasses of BaseMaster must provide a finish_job() method')

    @abstractmethod
    def subscribe_mq(self):
        raise NotImplementedError('subclasses of BaseMaster must provide a subscribe_mq() method')

    def start_subscribe_thread(self):
        """
            Start a new thread to subscribe message queue
        :return:
        """
        self.log.debug('start subscribe thread..')
        thd = threading.Thread(target=self.subscribe_mq)
        thd.setDaemon(True)
        thd.start()
