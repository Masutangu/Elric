# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)


class StopRequested(Exception):
    pass


class AlreadyRunningException(Exception):
    pass


class AddQueueFailed(Exception):
    pass


class JobAlreadyExist(Exception):
    pass


class JobDoesNotExist(Exception):
    pass


class AddFilterFailed(Exception):
    pass


class WrongType(Exception):
    pass