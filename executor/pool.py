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

            self.context.finish_job(job.id, False if f.exception() else True,
                                    str(f.exception_info()) if f.exception() else None,
                                    job.job_key, job.need_filter)
        future = self._pool.submit(job.func, *job.args, **job.kwargs)
        future.add_done_callback(job_done)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)
