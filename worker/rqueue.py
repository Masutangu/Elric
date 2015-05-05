__author__ = 'moxu'

from worker.base import ElricWorker
from rq import Queue, Connection, Worker
import redis

class RQWorker(ElricWorker):

    def __init__(self, host=None, port=None, url=None, listen_keys=None):
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)
        ElricWorker.__init__(self)
        self.listen_keys = []
        if listen_keys:
            self.listen_keys = listen_keys
        self.running = True

    def run(self):
        while self.running:
            with Connection(self.server):
                qs = map(Queue, self.listen_keys) or [Queue()]
                print qs
                w = Worker(qs)
                w.work()


    def submit_job(self, key, job):
        """
            submit job to master through rpc
            :param key:
            :param job:
            :return:
        """
        self.rpc_client.submit_job(key, job)

    def cancel_job(self, key, job):
        """
            cancel job to master through rpc
            :param key:
            :param job:
            :return:
        """
        self.rpc_client.cancel_job(key, job)

