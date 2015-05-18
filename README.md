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
RPC_HOST = 'localhost'
RPC_PORT = 8000

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
```

Create a master instance and start:

```python
rq_master = RQMaster()
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
rq_worker = RQWorker(name='test', listen_keys='job1') 
# create a job queue using add_queue() method. master will use `name`:`queue_key` as the final job key
rq_worker.add_queue('job1')
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

Todo list
---------
- *Add Monitor for worker*
- *Support job dependencies*



