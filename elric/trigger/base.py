# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseTrigger(object):
    __metaclass__ = ABCMeta

    def __init__(self, timezone):
        self.timezone = timezone

    @abstractmethod
    def get_next_trigger_time(self, previous_trigger_time):
        raise NotImplementedError

    @classmethod
    def create_trigger(cls, **trigger_args):
        return cls(**trigger_args)


