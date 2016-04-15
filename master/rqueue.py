# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from master.base import BaseMaster
from jobqueue.rqueue import RedisJobQueue
from core.exceptions import JobAlreadyExist, JobDoesNotExist, AlreadyRunningException
from jobstore.memory import MemoryJobStore
from jobstore.mongodb import MongoJobStore
from datetime import datetime
from tzlocal import get_localzone
from core.job import Job
from core.utils import timedelta_seconds
from threading import Event, RLock
from xmlrpclib import Binary
from settings import JOB_QUEUE_CONFIG, JOB_STORE_CONFIG
from Queue import Queue
import threading
import time


class RQMaster(BaseMaster):
    MAX_WAIT_TIME = 4294967  # Maximum value accepted by Event.wait() on Windows

    def __init__(self, timezone=None):
        BaseMaster.__init__(self)
        self.jobqueue = RedisJobQueue(self, **JOB_QUEUE_CONFIG)
        self.jobqueue_lock = RLock()
        self.timezone = timezone or get_localzone()
        self._event = Event()
        self._stopped = True
        self.jobstore = MongoJobStore(self, **JOB_STORE_CONFIG)
        #self.jobstore = MemoryJobStore(self)
        self.jobstore_lock = RLock()
        self._internal_buffer_queues = {}
        self._job_maximum_buffer_time = JOB_QUEUE_CONFIG['buffer_time']

    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        """
            Receive submit_job rpc request from worker.
            :type serialized_job str or xmlrpclib.Binary
            :type job_key str
            :type job_id str
            :type replace_exist bool
        """
        self.log.debug('client call submit job, id=%s, key=%s' % (job_id, job_key))
        if isinstance(serialized_job, Binary):
            serialized_job = serialized_job.data
        job_in_dict = Job.deserialize_to_dict(serialized_job)
        # if job doesn't contains trigger, then enqueue it into job queue immediately
        if not job_in_dict['trigger']:
            self._enqueue_job(job_key, serialized_job)
        # else store job into job store first
        else:
            # should I need a lock here?
            with self.jobstore_lock:
                try:
                    self.jobstore.add_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                except JobAlreadyExist as e:
                    if replace_exist:
                        self.jobstore.update_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                    else:
                        self.log.error(e)
            # wake up when new job has store into job store
            self.wake_up()

    def update_job(self, job_id, job_key, next_run_time, serialized_job):
        """
            Receive update_job rpc request from worker
            :type job_id: str
            :type job_key: str
            :type next_run_time datetime.datetime
            :type serialized_job str or xmlrpclib.Binary

        """
        if isinstance(serialized_job, Binary):
            serialized_job = serialized_job.data
        with self.jobstore_lock:
            try:
                self.jobstore.update_job(job_id, job_key=job_key, next_run_time=next_run_time,
                                         serialized_job=serialized_job)
            except JobDoesNotExist as e:
                self.log.error(e)

    def remove_job(self, job_id):
        """
            Receive remove_job rpc request from worker
            :type job_id: str
        """
        with self.jobstore_lock:
            try:
                self.jobstore.remove_job(job_id)
            except JobDoesNotExist:
                self.log.error('remove job error. job id %s does not exist' % job_id)

    def finish_job(self, job_id, is_success, details, filter_key=None, filter_value=None):
        """
            Receive finish_job rpc request from worker.
            :type job_id str
            :type is_success bool
            :type details str
            :type filter_key str or int
            :type filter_value str or int
        """
        with self.jobstore_lock:
            self.jobstore.save_execute_record(job_id, is_success, details)

    def _enqueue_buffer_queue(self, key, job):
        self.log.debug("job queue [%s] is full, put job into buffer queue" % key)
        try:
            self._internal_buffer_queues[key].put((job, datetime.now()))
        except KeyError:
            self._internal_buffer_queues[key] = Queue()
            self._internal_buffer_queues[key].put((job, datetime.now()))
            self.start_process_buffer_job(key)

    def _enqueue_job(self, key, job):
        """
            enqueue job into redis queue
            :type key: str
            :type job: str or xmlrpc.Binary
        """
        with self.jobqueue_lock:
            # check whether job queue is full
            if not self.jobqueue.is_full(key):
                self.jobqueue.enqueue(key, job)
            else:
                self._enqueue_buffer_queue(key, job)

    def start(self):
        """
            Start elric master. Select all due jobs from jobstore and enqueue them into redis queue.
            Then update due jobs' information into jobstore.
        :return:
        """
        if self.running:
            raise AlreadyRunningException
        self._stopped = False
        self.log.debug('eric master start...')

        while True:
            now = datetime.now(self.timezone)
            wait_seconds = None
            with self.jobstore_lock:
                for job_id, job_key, serialized_job in self.jobstore.get_due_jobs(now):
                    # enqueue due job into redis queue
                    self._enqueue_job(job_key, serialized_job)
                    # update job's information, such as next_run_time
                    job_in_dict = Job.deserialize_to_dict(serialized_job)
                    last_run_time = Job.get_serial_run_times(job_in_dict, now)
                    if last_run_time:
                        next_run_time = Job.get_next_trigger_time(job_in_dict, last_run_time[-1])
                        if next_run_time:
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

    def wake_up(self):
        self._event.set()

    @property
    def running(self):
        return not self._stopped

    def start_process_buffer_job(self, job_key):
        """
            Start filter data serialization thread
        """
        self.log.debug('start process buffer job... job key=[%s]' % job_key)
        thd = threading.Thread(target=self.process_buffer_job, args=(job_key, ))
        thd.setDaemon(True)
        thd.start()

    def process_buffer_job(self, job_key):
        while True:
            job, buffer_time = self._internal_buffer_queues[job_key].get()
            with self.jobqueue_lock:
                if not self.jobqueue.is_full(job_key):
                    self.jobqueue.enqueue(job_key, job)
                    continue

            if (datetime.now() - buffer_time).total_seconds() < self._job_maximum_buffer_time:
                self.log.debug("requeue into buffer")
                self._internal_buffer_queues[job_key].put((job, buffer_time))
                time.sleep(1.0)
            else:
                self.log.warning("timeout, discard job...")




