Elric
=====
A Simple Distributed Job Scheduler Based on Redis and Apscheduler. 

Features:
- ***Master-Slave Architecture***
- ***Distributed Job Based on Redis***
- ***Support Cron/Data/Interval/Immediate Job***

Usage
-----
Setup environment in `settings.py`:

```python
DISTRIBUTED_LOCK_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'db': 1,
    },
    'resource': 'elric_distributed_lock',
    'retry_count': 5,
    'retry_delay': 0.2,
}

JOB_QUEUE_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'db': 1,
    },
    'max_length': 100000,
    'buffer_time': 10
}

FILTER_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'db': 0,
    }
}

JOB_STORE_CONFIG = {
    'server': {},
    'maximum_records': 3
}
```

Create a master instance and start:

```python
rq_master = RQMasterExtend()
rq_master.start()
```

Implement some jobs/functions:

```python
def test_job(language=None):
    print 'my favorite language is {language}'.format(language=language)
```
Create a worker instace, specify worker's name and listening keys. Submit job and start worker.
```python
# worker will only receive job from listen_keys that have been provided here
rq_worker = RQWorker(name='test', listen_keys=['job1', ])
# submit job to master
rq_worker.submit_job(test_job, 'job1', kwargs={'language': 'python'})
# start worker, then worker will receive and execute job from master by listening job queue on listen keys you provided
rq_worker.start()
```

Running the example code
------------------------

This example illustrates how to submit different type of jobs to master.

Step 1. Setup environment in settings.py

Step 2. Start master
```
cd example
python test_master.py
```

Step 3. Start master
```
python test_worker.py
```

For more information
-------------
Documentation [described in this blog post](http://masutangu.com/2016/07/elric-documentation/).



