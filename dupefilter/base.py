# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseFilter(object):
    """
        baseclass of Filter
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def exist(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def add(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def clear(self, key):
        raise NotImplementedError