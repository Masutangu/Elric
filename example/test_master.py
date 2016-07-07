#-*- coding: utf-8 -*-
import sys
sys.path.append('..')
from master.rqextend import RQMasterExtend


if __name__ == '__main__':
    rq_master = RQMasterExtend()
    rq_master.start()
