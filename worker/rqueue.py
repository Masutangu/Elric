__author__ = 'moxu'

from time import sleep

import redis

from core.job import Job

from worker.base import ElricWorker
from jobqueue.rqueue import RedisJobQueue
import json
from concurrent.futures import ProcessPoolExecutor


class RQWorker(ElricWorker):

    def __init__(self, host=None, port=None, url=None, listen_keys=None, worker_num=2):
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)
        ElricWorker.__init__(self)
        self.listen_keys = []
        if listen_keys:
            self.listen_keys = listen_keys

        self._pool = ProcessPoolExecutor(max_workers=worker_num)
        self.running = True

    def run(self):
        try:
            while self.running:
                key, job_info = RedisJobQueue.dequeue_any(self.server, self.listen_keys)
                print 'get job %s from key %s' % (job_info, key)
                job = Job.deserialize(job_info)
                self.execute_job(job)

        except KeyboardInterrupt:
            print 'Eric worker quiting...'
            self.running = False

    def submit_job(self, key, func, args=None, kwargs=None):
        """
            submit job to master through rpc
            :param key:
            :param job:
            :return:
        """
        job_info = {
            'func': func,
            'args': tuple(args) if args is not None else (),
            'kwargs': dict(kwargs) if kwargs is not None else {},
        }
        job = Job(**job_info)
        self.rpc_client.submit_job(key, job.serialize())

    def cancel_job(self, key, job):
        """
            cancel job to master through rpc
            :param key:
            :param job:
            :return:
        """
        self.rpc_client.cancel_job(key, job)


    def execute_job(self, job):
        self._pool.submit(job.func,job.args, job.kwargs)

    def stop(self):
        self.running = False




