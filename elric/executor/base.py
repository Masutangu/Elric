# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseExecutor(object):
    __metaclass__ = ABCMeta

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def execute_job(self, job):
        raise NotImplementedError

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError
