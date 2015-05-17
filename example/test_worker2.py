# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
sys.path.append('..')
from worker.rqueue import RQWorker


def test_job():
    print 'hello this is worker 2'

if __name__ == '__main__':
    rq_worker = RQWorker(name='test2', listen_keys=['job1',])
    rq_worker.add_queue(['job1',])
    rq_worker.submit_job(test_job, 'job1', trigger='interval', minutes=1)
    rq_worker.start()
