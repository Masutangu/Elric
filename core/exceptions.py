# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)


class StopRequested(Exception):
    pass


class AlreadyRunningException(Exception):
    pass


class JobAlreadyExist(Exception):
    pass


class JobDoesNotExist(Exception):
    pass


class AddFilterFailed(Exception):
    pass


class WrongType(Exception):
    pass


class ParseConfigurationError(Exception):
    pass


def log_exception(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            self.context.log.error('func:[%s]() error! exception info [%s]' % (func.__name__, e))
    return wrapper