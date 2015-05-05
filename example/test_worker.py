__author__ = 'moxu'
import sys
sys.path.append('..')
from worker.rqueue import RQWorker

rq_worker = RQWorker(host='localhost', port=6379, listen_keys=['hello',])
rq_worker.submit_job('hello', 'example.job.complete_calculate')
#rq_worker.submit_job('hello', 'example.job.complete_calculate')
#rq_worker.submit_job('hello', 'example.job.complete_calculate')

rq_worker.run()
