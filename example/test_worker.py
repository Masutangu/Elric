__author__ = 'moxu'
import sys
sys.path.append('..')
from worker.rqueue import RQWorker
from example.job import hello_world

rq_worker = RQWorker(host='localhost', port=6379, listen_keys=['hello', 'world', 'test'])
rq_worker.submit_job('hello', 'example.job:hello_world')
rq_worker.submit_job('hello', hello_world)


rq_worker.run()
