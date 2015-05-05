__author__ = 'moxu'
import redis
from rq import Queue
from master.base import ElricMaster
from time import sleep

class RQMaster(ElricMaster):

    def __init__(self, host=None, port=None, url=None):
        self.queue_list = {}
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)
        ElricMaster.__init__(self)
        self.running = True


    def add_queue(self, key):
        if key not in self.queue_list.keys():
            self.queue_list[key] = Queue(key, connection=self.server)
            return self.queue_list[key]
        else:
            return None


    def submit_job(self, key, job):
        """
            receive rpc request from worker and save job into jobstore
            param key: key of work queue
            :param job: job
            :return: None
        """
        print 'client call submit job'
        self._enqueue_job(key, job)


    def cancel_job(self, key, job):
        """
            receive rpc request from worker and delete job from jobstore
            :param key: key of work queue
            :param job: job or job id
            :return: None
        """
        print 'client call cancel job'


    def _enqueue_job(self, key, job):
        """
            put job into work queue
            :param key: key of work queue
            :param job: job
            :return: None
        """
        self.queue_list[key].enqueue(job)


    def run(self):
        print 'master start...'
        while self.running:
            print 'master running ....'
            sleep(3)
            for k, q in self.queue_list.items():
                print q.jobs


