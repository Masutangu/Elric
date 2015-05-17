# -*- coding: utf-8 -*-
"""
    this file quote from apscheduler.trigger
"""
from __future__ import (absolute_import, unicode_literals)

from trigger.base import BaseTrigger
from datetime import timedelta, datetime
from core.utils import timedelta_seconds, astimezone, convert_to_datetime
from tzlocal import get_localzone
from math import ceil


class IntervalTrigger(BaseTrigger):

    def __init__(self, weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None, end_date=None, timezone=None):
        if timezone:
            timezone = astimezone(timezone)
        elif start_date and start_date.tzinfo:
            timezone = start_date.tzinfo
        elif end_date and end_date.tzinfo:
            timezone = end_date.tzinfo
        else:
            timezone = get_localzone()
        BaseTrigger.__init__(self, timezone)
        self.interval = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        self.interval_length = timedelta_seconds(self.interval)
        if self.interval_length == 0:
            self.interval = timedelta(seconds=1)
            self.interval_length = 1
        start_date = start_date or (datetime.now(self.timezone) + self.interval)
        self.start_date = convert_to_datetime(start_date, self.timezone, 'start_date')
        self.end_date = convert_to_datetime(end_date, self.timezone, 'end_date')

    def get_next_trigger_time(self, previous_trigger_time, now=None):
        if not now:
            curr_time = datetime.now(self.timezone)
        else:
            curr_time = now
        if previous_trigger_time:
            next_trigger_time = previous_trigger_time + self.interval
        elif self.start_date > curr_time:
            next_trigger_time = self.start_date
        else:
            timediff_seconds = timedelta_seconds(curr_time - self.start_date)
            next_interval_num = int(ceil(timediff_seconds / self.interval_length))
            next_trigger_time = self.start_date + self.interval * next_interval_num

        if not self.end_date or next_trigger_time <= self.end_date:
            return self.timezone.normalize(next_trigger_time)