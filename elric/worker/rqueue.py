# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import time
from multiprocessing import Queue
from tzlocal import get_localzone

from elric.jobqueue.rqueue import RedisJobQueue
from elric.executor.pool import ProcessPoolExecutor
from elric.trigger.tool import create_trigger
from elric.core import settings
from elric.core.job import Job
from elric.core.exceptions import AlreadyRunningException
from elric.worker.base import BaseWorker


class RQWorker(BaseWorker):
    def __init__(self, name, listen_keys=None, worker_num=2, timezone=None, logger_name='elric.worker'):
        BaseWorker.__init__(self, name, logger_name)
        self.jobqueue = RedisJobQueue(self, **settings.JOB_QUEUE_CONFIG)
        self.listen_keys = []
        if listen_keys:
            self.listen_keys = ['%s:%s' % (self.name, listen_key) for listen_key in listen_keys]
        self.timezone = timezone or get_localzone()
        self.internal_job_queue = Queue(maxsize=worker_num)
        self.executor = ProcessPoolExecutor(worker_num, self)
        self._stopped = True

    def start(self):
        if self.running:
            raise AlreadyRunningException

        self._stopped = False
        self.log.debug('elric worker running..')
        while self.running:
            try:
                # grab job from job queue only if internal_job_queue has space
                self.internal_job_queue.put("#", True)
                job_key, serialized_job = self.jobqueue.dequeue_any(self.listen_keys)
                job = Job.deserialize(serialized_job)
                self.log.debug('get job id=[%s] func=[%s]from key %s' % (job.id, job.func, job.job_key))
                self.executor.execute_job(job)
            except TypeError as e:
                self.log.error(e)
                time.sleep(60)
                continue

    def submit_job(self, func, job_key, args=None, kwargs=None, trigger=None, job_id=None,
                   replace_exist=False, need_filter=False, **trigger_args):
        """
            submit job to master through redis queue
            :type func: str or callable obj or unicode
            :type job_key: str or unicode
            :type args: tuple or list
            :type kwargs: dict
            :type trigger: str or unicode
            :type job_id: str or unicode
            :type replace_exist: bool
            :type trigger_args: dict
        """
        # use worker's timezone if trigger don't provide specific `timezone` configuration
        trigger_args['timezone'] = self.timezone
        job_in_dict = {
            'id': "%s:%s" % (self.name, job_id) if job_id else None,
            'func': func,
            'args': args,
            'trigger': create_trigger(trigger, trigger_args) if trigger else None,
            'kwargs': kwargs,
            'job_key': '%s:%s' % (self.name, job_key),
            'need_filter': need_filter,
            'replace_exist': replace_exist
        }
        job = Job(**job_in_dict)
        job.check()
        self.jobqueue.enqueue('__elric_submit_channel__', job.serialize())

    def remove_job(self, job_id):
        """
            send remove job request to master through redis queue
            :type job_id: str
        """
        job_in_dict = {
            'id': "%s:%s" % (self.name, job_id)
        }
        job = Job(**job_in_dict)
        self.jobqueue.enqueue('__elric_remove_channel__', job.serialize())

    def finish_job(self, job_id, is_success, details, job_key, need_filter):
        job_in_dict = {
            'id': job_id,
            'job_key': job_key,
            'is_success': is_success,
            'details': details,
            'need_filter': need_filter
        }
        job = Job(**job_in_dict)
        self.jobqueue.enqueue('__elric_finish_channel__', job.serialize())

    @property
    def running(self):
        return not self._stopped

    def stop(self):
        self.log.debug('Worker is quiting')
        self._stopped = True
        self.executor.shutdown()
