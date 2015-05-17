# -*- coding: utf-8 -*-
"""
    this file quote from apscheduler.trigger
"""
from __future__ import (absolute_import, unicode_literals)
from datetime import datetime
from tzlocal import get_localzone
from trigger.base import BaseTrigger
from core.utils import convert_to_datetime, datetime_repr, astimezone


class DateTrigger(BaseTrigger):

    def __init__(self, run_date=None, timezone=None):
        timezone = astimezone(timezone) or get_localzone()
        BaseTrigger.__init__(self, timezone)
        self.run_date = convert_to_datetime(run_date or datetime.now(), timezone, 'run_date')

    def get_next_trigger_time(self, previous_fire_time, now=None):
        return self.run_date if previous_fire_time is None else None

    def __str__(self):
        return 'date[%s]' % datetime_repr(self.run_date)

    def __repr__(self):
        return "<%s (run_date='%s')>" % (self.__class__.__name__, datetime_repr(self.run_date))
