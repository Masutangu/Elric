# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from executor.base import BaseExecutor
import concurrent.futures


class ProcessPoolExecutor(BaseExecutor):

    def __init__(self, max_workers, context):
        BaseExecutor.__init__(self, context)
        self._pool = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.context.log.debug('start executor..')

    def execute_job(self, job):
        def job_done(f):
            self.context.internal_job_queue.get(False)
            if f.exception():
                self.context.log.error('job [%s] occurs error. exception info [%s]' % (job.id, f.exception_info()))
            else:
                self.context.log.debug('job [%s] finish, result=[%s]' % (job.id, f.result()))
                if job.filter_key and job.filter_value:
                    self.context.rpc_client.call('finish_job', job.id, job.filter_key, job.filter_value)
        future = self._pool.submit(job.func, *job.args, **job.kwargs)
        future.add_done_callback(job_done)


    def shutdown(self, wait=True):
        self._pool.shutdown(wait)
