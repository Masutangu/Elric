import sys
sys.path.append('..')
from master.rqueue import RQMaster


if __name__ == '__main__':
    rq_master = RQMaster()
    #rq_master.add_queue('test:test')
    rq_master.start()