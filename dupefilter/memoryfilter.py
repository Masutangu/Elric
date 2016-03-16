# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from dupefilter.base import BaseFilter


class MemoryFilter(BaseFilter):

    def __init__(self):
        self.memory_filter = {}

    def exist(self, key, value):
        """
            check if value already exist
            if exist return 1
            if not exist return 0
        """
        return value in self.memory_filter.get(key, set())

    def add(self, key, value):
        self.memory_filter.setdefault(key, set()).add(value)

    def clear(self, key):
        """Clears fingerprints data"""
        self.memory_filter.pop(key, set())
