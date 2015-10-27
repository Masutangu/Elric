# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from dupefilter.base import BaseFilter


class RedisFilter(BaseFilter):

    def __init__(self, server, key):
        self.server = server
        self.key = key

    def exist(self, value):
        """
            check if value already exist
            if exist return 1
            if not exist return 0
        """
        return self.server.sismember(self.key, value)

    def add(self, value):
        added = self.server.sadd(self.key, value)
        return not added

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.server.delete(self.key)