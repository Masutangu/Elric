# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import redis
from core.job import Job
from core.exceptions import AlreadyRunningException
from xmlrpclib import Binary
from worker.base import BaseWorker
from jobqueue.rqueue import RedisJobQueue
from executor.pool import ProcessPoolExecutor
from tzlocal import get_localzone
from trigger.tool import create_trigger
from core.rpc import rpc_client_call


class RQWorker(BaseWorker):

    def __init__(self, name, host=None, port=None, url=None, listen_keys=None,
                                worker_num=2, logger=None, timezone=None):
        BaseWorker.__init__(self, name, logger)
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)
        self.listen_keys = []
        if listen_keys:
            self.listen_keys = ['%s:%s' % (self.name, listen_key) for listen_key in listen_keys]

        self.timezone = timezone or get_localzone()
        self.executor = ProcessPoolExecutor(max_workers=worker_num)
        self._stopped = True

    # def _install_signal_handlers(self):
    #     """
    #         Installs signal handlers for handling SIGINT and SIGTERM gracefully.
    #         quote from python-rq
    #     """
    #     def request_stop(signum, frame):
    #         self.log.debug('Got signal %s.' % signal_name(signum))
    #         raise StopRequested()
    #
    #     signal.signal(signal.SIGINT, request_stop)
    #     signal.signal(signal.SIGTERM, request_stop)

    def start(self):
        #self._install_signal_handlers()
        if self.running:
            raise AlreadyRunningException

        self._stopped = False
        self.log.debug('worker running..')
        while self.running:
            key, serialized_job = RedisJobQueue.dequeue_any(self.server, self.listen_keys)
            job = Job.deserialize(serialized_job)
            self.log.debug('get job id=[%s] func=[%s]from key %s' % (job.id, job.func, key))
            self.executor.execute_job(job)

    def submit_job(self, func, job_key, args=None, kwargs=None, trigger=None, job_id=None,
                    replace_exist=False, **trigger_args):
        """
            submit job to master through rpc
            :param key:
            :param job:
            :return:
        """
        # use worker's timezone if trigger don't provide specific `timezone` configuration
        job_key = '%s:%s' % (self.name, job_key)
        trigger_args['timezone'] = self.timezone
        job_in_dict = {
            'id': job_id,
            'func': func,
            'args': args,
            'trigger': create_trigger(trigger, trigger_args) if trigger else None,
            'kwargs': kwargs,
        }
        job = Job(**job_in_dict)
        rpc_client_call(self.rpc_host, self.rpc_port, 'submit_job', Binary(job.serialize()),
                        job_key, job.id, replace_exist)
        #self.rpc_client.submit_job(Binary(job.serialize()), job_key, job.id, replace_exist)

    def update_job(self, job_id, job_key, next_run_time, serialized_job):
        job_key = '%s:%s' % (self.name, job_key)
        rpc_client_call(self.rpc_host, self.rpc_port, 'update_job', job_id, job_key,
                        next_run_time, Binary(serialized_job))
        #self.rpc_client.update_job(job_id, job_key, next_run_time, Binary(serialized_job))

    def remove_job(self, job_id):
        """
            cancel job to master through rpc
            :param key:
            :param job:
            :return:
        """
        rpc_client_call(self.rpc_host, self.rpc_port, 'remove_job', job_id)
        #self.rpc_client.remove_job(job_id)

    @property
    def running(self):
        return not self._stopped

    def stop(self):
        self.log.debug('Worker is quiting')
        self._stopped = True
        self.executor.shutdown()








