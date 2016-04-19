# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from jobqueue.base import JobQueue
from core.exceptions import WrongType
import redis


class RedisJobQueue(JobQueue):
    def __init__(self, context, **config):
        JobQueue.__init__(self, context)
        self.server = redis.Redis(**config['server'])
        self.max_length = config['max_length']

    def __len__(self, key):
        return self.server.llen(key)

    def enqueue(self, key, value):
        return self.server.lpush(key, value)

    def dequeue(self, key, timeout=0):
        data = self.server.brpop(key, timeout)
        if isinstance(data, tuple):
            data = data[1]
        if data:
            return data

    def dequeue_any(self, queue_keys, timeout=0):
        if not isinstance(queue_keys, (tuple, list)):
            raise WrongType('queue_keys: [%s] must be tuple or list' % queue_keys)
        result = self.server.brpop(queue_keys, timeout)
        if result:
            queue_key, data = result
            return queue_key, data

    def is_full(self, key):
        return self.server.llen(key) >= self.max_length

    def clear(self, key):
        self.server.delete(key)