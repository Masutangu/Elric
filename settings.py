# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)


JOB_QUEUE_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'db': 1,
    },
    'max_length': 3,
    'buffer_time': 60
}

FILTER_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'db': 1,
    }
}

RPC_CONFIG = {
    'server': {
        'host': 'localhost',
        'port': 8000,
    }
}

JOB_STORE_CONFIG = {
    'server': {},
    'maximum_records': 3
}

LOGGINGF_CONFIG = {
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


