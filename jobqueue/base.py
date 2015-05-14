# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class JobQueue(object):
    """
        baseclass of job queue
        redis job queue/zmq job queue or customer job queue should inherit from this class
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abstractmethod
    def enqueue(self, job):
        raise NotImplementedError

    @abstractmethod
    def dequeue(self, timeout=0):
        raise NotImplementedError

    @abstractmethod
    def clear(self):
        raise NotImplementedError
