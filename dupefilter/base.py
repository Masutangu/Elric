# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseFilter(object):
    """
        baseclass of Filter
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def exist(self, value):
        raise NotImplementedError


    @abstractmethod
    def clear(self):
        raise NotImplementedError