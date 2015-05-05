__author__ = 'moxu'
import sys
sys.path.append('..')
from master.rqueue import RQMaster

rq_master = RQMaster(host='localhost', port=6379)
queue = rq_master.add_queue('hello')

rq_master.run()