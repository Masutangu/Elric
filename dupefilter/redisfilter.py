# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from dupefilter.base import BaseFilter


class RedisFilter(BaseFilter):

    def __init__(self, server):
        self.server = server

    def exist(self, key, value):
        """
            check if value already exist
            if exist return 1
            if not exist return 0
        """
        return self.server.sismember(key, value)

    def add(self, key, value):
        self.server.sadd(key, value)

    def clear(self, key):
        """Clears fingerprints data"""
        self.server.delete(key)