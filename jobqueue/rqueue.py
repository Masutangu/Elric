__author__ = 'Masutangu'

from jobqueue.base import JobQueue

class RedisJobQueue(JobQueue):

    def __init__(self, server, key):
        self.server = server
        self.key = key

    def __len__(self):
        return self.server.llen(self.key)

    def enqueue(self, job):
        self.server.lpush(self.key, job)

    def dequeue(self, timeout=0):
        data = self.server.brpop(self.key, timeout)
        if isinstance(data, tuple):
            data = data[1]
        if data:
            return data

    @classmethod
    def dequeue_any(cls, server, queue_keys, timeout=0):
        result = server.blpop(queue_keys, timeout)
        if result:
            queue_key, data = result
            return queue_key, data


    def clear(self):
        self.server.delete(self.key)