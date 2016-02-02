# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class JobQueue(object):
    """
        baseclass of job queue
        redis job queue/zmq job queue or customer job queue should inherit from this class
    """
    __metaclass__ = ABCMeta

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def __len__(self, key):
        raise NotImplementedError

    @abstractmethod
    def enqueue(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def dequeue(self, key, timeout=0):
        raise NotImplementedError

    @abstractmethod
    def dequeue_any(self, queue_keys, timeout=0):
        raise NotImplementedError

    @abstractmethod
    def clear(self, key):
        raise NotImplementedError


