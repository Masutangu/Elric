# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

RPC_HOST = 'localhost'
RPC_PORT = 8000

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

SETTING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s]-[%(levelname)s]-[%(filename)s %(funcName)s():line %(lineno)s]-[%(message)s]',
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'worker': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'standard',
            'filename': 'worker.log',
            'maxBytes': 1024*1024,
            'backupCount': 5,
        },
        'master': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'standard',
            'filename': 'master.log',
            'maxBytes': 1024*1024,
            'backupCount': 5,
        }
    },
    'loggers': {
        'elric.master': {
            'handlers': ['console', "master"],
            'level': 'DEBUG',
        },
        'elric.worker': {
            'handlers': ['console', "worker"],
            'level': 'DEBUG',
        },
    }
}


