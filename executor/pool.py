# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from executor.base import BaseExecutor
import concurrent.futures
from core.exceptions import StopRequested
from core.rpc import rpc_client_call


class ProcessPoolExecutor(BaseExecutor):

    def __init__(self, max_workers, context):
        BaseExecutor.__init__(self, context)
        self._pool = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.context.log.debug('start executor..')

    def execute_job(self, job):
        try:
            def job_done(f):
                if f.exception():
                    self.context.log.error('job [%s] occurs error. exception info [%s]' % (job.id, f.exception_info()))
                else:
                    self.context.log.debug('job [%s] finish, result=[%s]' % (job.id, f.result()))
                    if job.filter_key and job.filter_value:
                        rpc_client_call('finish_job', job.id, job.filter_key, job.filter_value)
            future = self._pool.submit(job.func, *job.args, **job.kwargs)
            future.add_done_callback(job_done)
        except StopRequested:
            self.context.log.warning('executor quit...')

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)
