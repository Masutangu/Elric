# -*- coding: utf-8 -*-
from __future__ import absolute_import

from jobstore.base import BaseJobStore
from core.utils import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from core.exceptions import JobAlreadyExist, JobDoesNotExist


class MemoryJobStore(BaseJobStore):
    def __init__(self, logger):
        super(MemoryJobStore, self).__init__(logger)
        self.job_info = {}
        self.job_run_time = []

    def add_job(self, job_id, job_key, next_run_time, serialized_job):
        """
            save job
            :type job_id: str
            :type job_key: str
            :type next_run_time: datetime.datetime
            :type serialized_job: str or xmlrpclib.Binary
        """
        if job_id in self.job_info:
            raise JobAlreadyExist
        next_timestamp = datetime_to_utc_timestamp(next_run_time)
        index = self._get_job_index(job_id, next_timestamp)
        self.job_run_time.insert(index, (job_id, next_timestamp))
        self.job_info[job_id] = {'serialized_job': serialized_job, 'job_key': job_key, 'status': 0,
                                 'next_timestamp': next_timestamp}

    def update_job(self, job_id, job_key=None, next_run_time=None, serialized_job=None, status=None):
        """
            update job
            :type job_id: str
            :type job_key: str
            :type next_run_time: datetime.datetime
            :type serialized_job: str or xmlrpclib.Binary
            :type status: int
        """
        self.log.debug("update job %s next run time=%s" % (job_id, next_run_time))
        self.log.debug("job_run_time = %s" % self.job_run_time)
        if job_id not in self.job_info:
            raise JobDoesNotExist
        job_info = self.job_info[job_id]
        if job_key is not None:
            job_info['job_key'] = job_key
        if serialized_job is not None:
            job_info['serialized_job'] = serialized_job
        if status is not None:
            job_info['status'] = status

        new_timestamp = datetime_to_utc_timestamp(next_run_time)
        old_timestamp = job_info['next_timestamp']
        if new_timestamp != old_timestamp:
            old_index = self._get_job_index(job_id, old_timestamp)
            del self.job_run_time[old_index]
            new_index = self._get_job_index(job_id, new_timestamp)
            self.job_run_time.insert(new_index, (job_id, new_timestamp))

        self.log.debug("job_run_time = %s" % self.job_run_time)

    def remove_job(self, job_id):
        self.log.debug("before remove job run time = %s" % self.job_run_time)
        """
            remove job
            :type job_id: str
        """
        if job_id not in self.job_info:
            raise JobDoesNotExist
        job_info = self.job_info[job_id]
        index = self._get_job_index(job_id, job_info['next_timestamp'])
        del self.job_info[job_id]
        del self.job_run_time[index]
        self.log.debug("after remove job run time = %s" % self.job_run_time)

    def get_due_jobs(self, now):
        """
            Get due jobs.
            :type now: datetime.datetime
        """
        curr_timestamp = datetime_to_utc_timestamp(now)
        for job_id, timestamp in self.job_run_time:
            if timestamp is None or timestamp > curr_timestamp:
                break
            job_info = self.job_info[job_id]
            if job_info['status'] == 0:
                yield (job_id, job_info['job_key'], job_info['serialized_job'])

    def get_closest_run_time(self):
        return utc_timestamp_to_datetime(self.job_run_time[0][1]) if self.job_run_time else None

    def _get_job_index(self, job_id, timestamp):
        """
            insert job_id and job_run_time into self.job_run_time sorted by job_run_time
            and return index
            :type job_id: str
            :type timestamp: float
        """
        start, end = 0, len(self.job_run_time)
        timestamp = float('inf') if timestamp is None else timestamp
        while start < end:
            mid = (start + end) / 2
            mid_job_id, mid_timestamp = self.job_run_time[mid]
            mid_timestamp = float('inf') if mid_timestamp is None else mid_timestamp
            if mid_timestamp > timestamp:
                end = mid
            elif mid_timestamp < timestamp:
                start = mid + 1
            elif mid_job_id > job_id:
                end = mid
            elif mid_job_id < job_id:
                start = mid + 1
            else:
                return mid

        return start





