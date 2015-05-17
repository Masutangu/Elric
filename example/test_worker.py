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


def test_date_job():
    print 'hello i am date job'


def test_cron_job():
    print 'hello i am crontab job'


if __name__ == '__main__':
    rq_worker = RQWorker(name='test', listen_keys=['job1', 'job2'])
    rq_worker.add_queue(['job1', 'job2'])
    rq_worker.submit_job(test_date_job, 'job1', trigger='date', run_date='2015-05-17 21:13:30')
    rq_worker.submit_job(wapper_job, 'job1', trigger='interval', seconds=30)
    rq_worker.submit_job(test_job, 'job2', trigger='interval', seconds=8, kwargs={'language': 'python'})
    rq_worker.submit_job(test_cron_job, 'job2', trigger='cron', second=7)
    rq_worker.start()
