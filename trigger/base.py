# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseTrigger(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_next_trigger_time(self, previous_trigger_time):
        raise NotImplementedError

    @classmethod
    def create_trigger(cls, **triger_args):
        raise NotImplementedError


