__author__ = 'Masutangu'
from abc import ABCMeta, abstractmethod


class BaseTrigger(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_next_run_time(self, previous_fire_time, curr_time):
        raise NotImplementedError

    @classmethod
    def create_trigger(cls, **triger_args):
        raise NotImplementedError