__author__ = 'moxu'

from abc import ABCMeta, abstractmethod


class JobQueue(object):
    __metaclass__ = ABCMeta
    """
        baseclass of job queue
        redis job queue/zmq job queue or customer job queue should inherit from this class
    """
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
