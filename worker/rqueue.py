__author__ = 'Masutangu'


import redis
import signal
from core.utils import signal_name
from core.job import Job
from core.exceptions import StopRequested, AlreadyRunningException
from xmlrpclib import Binary
from worker.base import BaseWorker
from jobqueue.rqueue import RedisJobQueue
from executor.pool import ProcessPoolExecutor


class RQWorker(BaseWorker):

    def __init__(self, host=None, port=None, url=None, listen_keys=None, worker_num=2, logger=None):
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)
        self.listen_keys = []
        if listen_keys:
            self.listen_keys = listen_keys
        self.executor = ProcessPoolExecutor(max_workers=worker_num)
        self._stopped = True
        BaseWorker.__init__(self, logger)

    def _install_signal_handlers(self):
        """
            Installs signal handlers for handling SIGINT and SIGTERM gracefully.
            quote from python-rq
        """
        def request_stop(signum, frame):
            self.log.debug('Got signal %s.' % signal_name(signum))
            raise StopRequested()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def run(self):
        self._install_signal_handlers()
        if self.running:
            raise AlreadyRunningException
        try:
            print 'worker running..'
            while True:
                key, job_info = RedisJobQueue.dequeue_any(self.server, self.listen_keys)
                print 'get job %s from key %s' % (job_info, key)
                job = Job.deserialize(job_info)
                print 'job.func', job.func
                self.executor.execute_job(job)

        except StopRequested:
            print 'worker stopped'
            self.stop()

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
        #self.rpc_client.submit_job(key, decode_binary_data(job.serialize()))
        self.rpc_client.submit_job(key, Binary(job.serialize()))

    def cancel_job(self, key, job):
        """
            cancel job to master through rpc
            :param key:
            :param job:
            :return:
        """
        self.rpc_client.cancel_job(key, job)

    @property
    def running(self):
        return not self._stopped

    def stop(self):
        print 'Worker is quiting'
        self._stopped = True
        self.executor.shutdown()








