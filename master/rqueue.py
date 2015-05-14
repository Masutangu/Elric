# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import redis
from master.base import BaseMaster
from jobqueue.rqueue import RedisJobQueue
import six
from collections import Iterable
from core.exceptions import AddQueueFailed, JobAlreadyExist, JobDoesNotExist
from jobstore.sqlalchemy2 import SQLAlchemyJobStore
from jobstore.memory import MemoryJobStore
from datetime import datetime
from tzlocal import get_localzone
from core.job import Job
from core.utils import timedelta_seconds
from threading import Event, RLock
from xmlrpclib import Binary


class RQMaster(BaseMaster):

    MAX_WAIT_TIME = 4294967  # Maximum value accepted by Event.wait() on Windows

    def __init__(self, host=None, port=None, url=None, timezone=None, logger=None):
        BaseMaster.__init__(self, logger)
        self.queue_list = {}
        if url:
            self.server = redis.from_url(url)
        else:
            self.server = redis.Redis(host=host, port=port)

        self.timezone = timezone or get_localzone()
        self._event = Event()
        self.running = True
        #self.jobstore = SQLAlchemyJobStore("sqlite:///jobs.sqlite?check_same_thread=False")
        self.jobstore = MemoryJobStore()
        self.jobstore_lock = RLock()

    def add_queue(self, keys):
        if isinstance(keys, six.string_types):
            keys = [keys, ]
        if not isinstance(keys, Iterable):
            raise AddQueueFailed
        for key in keys:
            if key not in self.queue_list.keys():
                self.queue_list[key] = RedisJobQueue(self.server, key)

    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        """
            receive rpc request from worker and save job into jobstore
            param key: key of work queue
            :param job: job
            :return: None
        """
        # should I need a lock here?
        self.log.debug('client call submit job %s' % job_id)
        if isinstance(serialized_job, Binary):
            serialized_job = serialized_job.data
        job_in_dict = Job.deserialize_to_dict(serialized_job)
        if not job_in_dict['trigger']:
            self._enqueue_job(job_key, serialized_job)
        else:
            with self.jobstore_lock:
                try:
                    self.jobstore.add_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                except JobAlreadyExist:
                    if replace_exist:
                        self.jobstore.update_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                    else:
                        self.log.warning('job %s alread exist' % job_id)
            self.wakeup()

    def update_job(self, job_id, job_key, next_run_time, serialized_job, status=0):
        """
            receive rpc request from worker and update job from jobstore
            :param key: key of work queue
            :param job: job or job id
            :return: None
        """
        try:
            if isinstance(serialized_job, Binary):
                serialized_job = serialized_job.data
            self.jobstore.update_job(job_id, job_key=job_key, next_run_time=next_run_time,
                                     serialized_job=serialized_job, status=status)
        except JobDoesNotExist:
            self.log.error('job %s does not exist' % job_id)

    def remove_job(self, job_id):
        """
            receive rpc request from worker and delete job from jobstore
            :param key: key of work queue
            :param job: job or job id
            :return: None
        """
        self.jobstore.remove_job(job_id)

    def _enqueue_job(self, key, job):
        """
            put job into work queue
            :param key: key of work queue
            :param job: job
            :return: None
        """
        self.queue_list[key].enqueue(job)

    def start(self):
        self.log.debug('master start...')

        while self.running:
            now = datetime.now(self.timezone)
            wait_seconds = None
            for job_id, job_key, serialized_job in self.jobstore.get_due_jobs(now):
                # enqueue job into redis queue
                self._enqueue_job(job_key, serialized_job)
                job_in_dict = Job.deserialize_to_dict(serialized_job)
                last_run_time = Job.get_serial_run_times(job_in_dict, now)
                if last_run_time:
                    next_run_time = Job.get_next_trigger_time(job_in_dict, last_run_time[-1])
                    if next_run_time:
                        # update job next run time in jobstore
                        job_in_dict['next_run_time'] = next_run_time
                        self.update_job(job_id, job_key, next_run_time, Job.dict_to_serialization(job_in_dict))
                    else:
                        # if job has no next run time, then remove it from jobstore
                        self.remove_job(job_id=job_id)

            # get next closet run time job from jobstore and set it to be wake up time
            closest_run_time = self.jobstore.get_closest_run_time()
            if closest_run_time is not None:
                wait_seconds = max(timedelta_seconds(closest_run_time - now), 0)
                self.log.debug('Next wakeup is due at %s (in %f seconds)' % (closest_run_time, wait_seconds))
            self._event.wait(wait_seconds if wait_seconds is not None else self.MAX_WAIT_TIME)
            self._event.clear()

    def wakeup(self):
        self._event.set()



