# -*- coding:utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from master.rqbase import RQMasterBase
from dupefilter.redisfilter import RedisFilter
from core.exceptions import JobAlreadyExist
from threading import RLock
from settings import FILTER_CONFIG, DISTRIBUTED_LOCK_CONFIG
from core.lock import distributed_lock


class RQMasterExtend(RQMasterBase):
    def __init__(self, timezone=None):
        RQMasterBase.__init__(self, timezone)
        self.filter = RedisFilter(**FILTER_CONFIG)
        self.filter_lock = RLock()

    def submit_job(self, job):
        def exist(key, value):
            with self.filter_lock:
                return self.filter.exist(key, value)

        self.log.debug("client call submit job [%s]" % job.id)

        if job.need_filter:
            if exist(job.filter_key, job.id):
                self.log.debug("job [%s] has been filter..." % job.id)
                return False

        if not job.trigger:
            self._enqueue_job(job.job_key, job.serialize())
        else:
            with distributed_lock(**DISTRIBUTED_LOCK_CONFIG):
                try:
                    self.jobstore.add_job(job)
                except JobAlreadyExist:
                    if job.replace_exist:
                        self.jobstore.update_job(job)
                    else:
                        self.log.warn('job [%s] already exist' % job.id)
            self.wake_up()

        return True

    def finish_job(self, job):
        """
            Receive finish_job rpc request from worker.
            :type job_id str
            :type is_success bool
            :type details str
            :type filter_key str or int
            :type filter_value str or int
        """
        self.log.debug("job_id [%s] finish" % job.id)
        RQMasterBase.finish_job(self, job)
        # add job into filter only when job is finish successfully
        if job.is_success and job.need_filter:
            self.filter.add(job.filter_key, job.id)

