# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from logging.config import dictConfig

from elric.core import settings


DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s]-[%(levelname)s]-[%(filename)s %(funcName)s():line %(lineno)s]-[%(message)s]',
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        'elric.master': {
            'handlers': ['console'],
            'level': 'INFO',
        },
        'elric.worker': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }
}


def init_logging_config():
    config = DEFAULT_LOGGING
    try:
        if settings.LOGGING_CONFIG:
            config = settings.LOGGING_CONFIG
    except AttributeError as e:
        pass

    dictConfig(config)
