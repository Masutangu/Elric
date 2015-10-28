# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from dupefilter.base import BaseFilter


class MemoryFilter(BaseFilter):

    def __init__(self):
        self.memorey_filter = set()

    def exist(self, value):
        """
            check if value already exist
            if exist return 1
            if not exist return 0
        """
        return value in self.memorey_filter

    def add(self, value):
        self.memorey_filter.add(value)

    def clear(self):
        """Clears fingerprints data"""
        self.memorey_filter.clear()
