# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from elric.trigger.date import DateTrigger
from elric.trigger.interval import IntervalTrigger
from elric.trigger.cron import CronTrigger


def create_trigger(trigger_name, trigger_args):
    trigger_class = {
        'interval': IntervalTrigger,
        'date': DateTrigger,
        'cron': CronTrigger
    }[trigger_name]

    return trigger_class.create_trigger(**trigger_args)