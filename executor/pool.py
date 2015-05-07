__author__ = 'Masutangu'

from executor.base import BaseExecutor
import concurrent.futures


class ProcessPoolExecutor(BaseExecutor):

    def __init__(self, max_workers, logger=None):
        BaseExecutor.__init__(self, logger)
        self._pool = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.log.debug('start executor..')

    def execute_job(self, job):
        def job_done(f):
            if f.exception():
                self.log.error('job %s occurs error. exception info %s' % (job, f.exection_info()))
                #print 'job %s occurs error. exception info %s' % (job, f.exection_info())
            else:
                self.log.debug('job %s finish, result=%s' % (job, f.result()))
                #print 'job %s finish, result=%s' % (job, f.result())
        future = self._pool.submit(job.func, *job.args, **job.kwargs)
        future.add_done_callback(job_done)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)
