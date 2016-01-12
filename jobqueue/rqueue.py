# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from jobqueue.base import JobQueue
from core.exceptions import log_exception


class RedisJobQueue(JobQueue):
    def __init__(self, server, context):
        JobQueue.__init__(self, context)
        self.server = server

    @log_exception
    def __len__(self, key):
        return self.server.llen(key)

    @log_exception
    def enqueue(self, key, value):
        self.server.lpush(key, value)

    @log_exception
    def dequeue(self, key, timeout=0):
        data = self.server.brpop(key, timeout)
        if isinstance(data, tuple):
            data = data[1]
        if data:
            return data

    @log_exception
    def dequeue_any(self, queue_keys, timeout=0):
        result = self.server.brpop(queue_keys, timeout)
        if result:
            queue_key, data = result
            return queue_key, data

    @log_exception
    def clear(self, key):
        self.server.delete(key)