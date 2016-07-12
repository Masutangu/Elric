#-*- coding: utf-8 -*-

import os
os.environ.setdefault('ELRIC_SETTINGS_MODULE', 'settings')

from elric.master.rqextend import RQMasterExtend


if __name__ == '__main__':
    rq_master = RQMasterExtend()
    rq_master.start()
