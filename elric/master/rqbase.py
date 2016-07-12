# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)


from datetime import datetime
from tzlocal import get_localzone
from threading import Event, RLock
import threading
import time
from Queue import Queue

from elric.master.base import BaseMaster
from elric.jobqueue.rqueue import RedisJobQueue
from elric.core.exceptions import JobAlreadyExist, JobDoesNotExist, AlreadyRunningException
# from jobstore.memory import MemoryJobStore
from elric.jobstore.mongodb import MongoJobStore
from elric.core.job import Job
from elric.core.utils import timedelta_seconds
from elric.core import settings
from elric.core.lock import distributed_lock


class RQMasterBase(BaseMaster):

    MIN_WAIT_TIME = 5

    def __init__(self, timezone=None):
        BaseMaster.__init__(self)
        self.jobqueue = RedisJobQueue(self, **settings.JOB_QUEUE_CONFIG)
        self.jobqueue_lock = RLock()
        self.timezone = timezone or get_localzone()
        self._event = Event()
        self._stopped = True
        self.jobstore = MongoJobStore(self, **settings.JOB_STORE_CONFIG)
        #self.jobstore = MemoryJobStore(self)
        self._internal_buffer_queues = {}
        self._job_maximum_buffer_time = settings.JOB_QUEUE_CONFIG['buffer_time']

    def submit_job(self, job):
        """
            process submit_job request from worker.
            :type job: Job
        """
        self.log.debug('client submit job, id=%s, key=%s' % (job.id, job.job_key))
        # if job doesn't contains trigger, then enqueue it into job queue immediately
        if not job.trigger:
            self._enqueue_job(job.job_key, job.serialize())
        # else store job into job store first
        else:
            with distributed_lock(**settings.DISTRIBUTED_LOCK_CONFIG):
                try:
                    self.jobstore.add_job(job)
                except JobAlreadyExist as e:
                    if job.replace_exist:
                        self.update_job(job)
                    else:
                        self.log.error(e)
            # wake up when new job has store into job store
            self.wake_up()

    def update_job(self, job):
        """
            update job in jobstore
            :type job: Job
        """
        try:
            self.jobstore.update_job(job)
        except JobDoesNotExist as e:
            self.log.error(e)

    def remove_job(self, job):
        """
            remove job from jobstore
            :type job: Job
        """
        try:
            self.jobstore.remove_job(job)
        except JobDoesNotExist:
            self.log.error('remove job error. job id %s does not exist' % job.id)

    def finish_job(self, job):
        """
            process finish_job request from worker
            :type job: Job
        """
        self.jobstore.save_execute_record(job)

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
            enqueue job into jobqueue
            :type key: str
            :type job: str or xmlrpc.Binary
        """
        self.log.debug('enqueue job key=[%s]' % key)
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

        self.start_subscribe_thread()

        while True:
            now = datetime.now(self.timezone)
            wait_seconds = None
            with distributed_lock(**settings.DISTRIBUTED_LOCK_CONFIG):
                for job_id, job_key, serialized_job in self.jobstore.get_due_jobs(now):
                    # enqueue due job into redis queue
                    self._enqueue_job(job_key, serialized_job)
                    # update job's information, such as next_run_time
                    job = Job.deserialize(serialized_job)
                    last_run_time = Job.get_serial_run_times(job, now)
                    if last_run_time:
                        next_run_time = Job.get_next_trigger_time(job, last_run_time[-1])
                        if next_run_time:
                            job.next_run_time = next_run_time
                            self.update_job(job)
                        else:
                            # if job has no next run time, then remove it from jobstore
                            self.remove_job(job)

                # get next closet run time job from jobstore and set it to be wake up time
                closest_run_time = self.jobstore.get_closest_run_time()

            if closest_run_time is not None:
                wait_seconds = min(max(timedelta_seconds(closest_run_time - now), 0), self.MIN_WAIT_TIME)
                self.log.debug('Next wakeup is due at %s (in %f seconds)' % (closest_run_time, wait_seconds))
            self._event.wait(wait_seconds if wait_seconds is not None else self.MIN_WAIT_TIME)
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

    def subscribe_mq(self):
        while self.running:
            try:
                # grab job from job queue only if internal_job_queue has space
                key, serialized_job = self.jobqueue.dequeue_any(['__elric_submit_channel__',
                                                                 '__elric_remove_channel__',
                                                                 '__elric_finish_channel__'])
                Job.deserialize(serialized_job)
                self.func_map[key](Job.deserialize(serialized_job))
            except TypeError as e:
                self.log.error(e)
                time.sleep(60)
                continue



