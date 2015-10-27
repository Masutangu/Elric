# -*- coding:utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from master.rqueue import RQMaster
from dupefilter.redisfilter import RedisFilter
from core.exceptions import JobAlreadyExist
from xmlrpclib import Binary
from core.job import Job
from threading import RLock


class Spider(RQMaster):
    def __init__(self, timezone=None):
        RQMaster.__init__(self, timezone)
        self.filter_list = {}
        self.filter_lock = RLock()
        self.rpc_server.register_function(self.finish_job, 'finish_job')

    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        def exist(key, value):
            with self.filter_lock:
                try:
                    return self.filter_list[key].exist(value)
                except KeyError:
                    self.filter_list[key] = RedisFilter(self.server, "%s:dupefilter" % key)
                    return self.filter_list[key].exist(value)

        self.log.debug("client call submit job %s" % job_id)

        if isinstance(serialized_job, Binary):
            serialized_job = serialized_job.data

        job_in_dict = Job.deserialize_to_dict(serialized_job)
        filter_key = job_in_dict['filter_key']
        filter_value = job_in_dict['filter_value']

        if filter_key and filter_value:
            if exist(filter_key, filter_value):
                self.log.debug("%s has been filter..." % filter_value)
                return False

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
                        self.log.warn('job %s already exist' % job_id)
            self.wake_up()

        return True

    def finish_job(self, job_id, key, value):
        self.log.debug("job_id [%s] finish" % job_id)
        if key and value:
            with self.filter_lock:
                try:
                    self.filter_list[key].add(value)
                except KeyError:
                    self.filter_list[key] = RedisFilter(self.server, "%s:dupefilter" % key)
                    return self.filter_list[key].add(value)