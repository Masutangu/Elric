# -*- coding=utf-8 -*-
from __future__ import absolute_import, unicode_literals

import importlib
import os

from elric.core.exceptions import ImproperlyConfigured

ENVIRONMENT_VARIABLE = "ELRIC_SETTINGS_MODULE"


class Settings(object):
    def _setup(self):
        settings_module = os.environ.get(ENVIRONMENT_VARIABLE)
        if not settings_module:
            raise ImproperlyConfigured("You must define the environment variable {}".format(ENVIRONMENT_VARIABLE))

        self.SETTINGS_MODULE = settings_module

        s = importlib.import_module(self.SETTINGS_MODULE)
        required_settings = ('JOB_QUEUE_CONFIG', 'JOB_STORE_CONFIG', 'FILTER_CONFIG', 'DISTRIBUTED_LOCK_CONFIG')
        for setting in required_settings:
            if not getattr(s, setting, None):
                raise ImproperlyConfigured("'{}' setting is not defined properly".format(setting))

        self.settings = s

    def __getattr__(self, item):
        return getattr(self.settings, item)

    def __init__(self):
        self._setup()

settings = Settings()
