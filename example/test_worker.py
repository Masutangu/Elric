# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
sys.path.append('..')
from worker.rqueue import RQWorker


def wapper_job():
    print 'run first job'
    rq_worker.submit_job(nest_job, 'job1', args=['hi i am nested job'])


def nest_job(welcome):
    print welcome


def test_job(language=None):
    print 'my favorite language is {language}'.format(language=language)


if __name__ == '__main__':
    rq_worker = RQWorker(name='test', listen_keys=['job1', 'job2'])
    rq_worker.add_queue(['job1', 'job2'])
    rq_worker.submit_job(wapper_job, 'job1', trigger='interval', seconds=30)
    rq_worker.submit_job(test_job, 'job2', trigger='interval', seconds=8, kwargs={'language': 'python'})
    rq_worker.start()
