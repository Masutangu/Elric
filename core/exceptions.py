# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)


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

