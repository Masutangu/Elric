# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

import redlock
import time


class distributed_lock(object):
    def __init__(self, **config):
        self.config = config
        self.dlm = redlock.Redlock([config['server'], ],
                                   retry_count=config['retry_count'],
                                   retry_delay=config['retry_delay'])
        self.dlm_lock = None

    def __enter__(self):
        while not self.dlm_lock:
            self.dlm_lock = self.dlm.lock(self.config['resource'], 1000)
            if self.dlm_lock:
                break
            else:
                time.sleep(self.config['retry_delay'])

    def __exit__(self, type, value, traceback):
        self.dlm.unlock(self.dlm_lock)
        self.dlm_lock = None
