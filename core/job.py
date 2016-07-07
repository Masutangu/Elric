# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from core.utils import ref_to_obj, obj_to_ref, check_callable_args, convert_to_datetime
from uuid import uuid4
from trigger.base import BaseTrigger
from core.exceptions import WrongType
import six
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Job(object):

    def __init__(self, **job_in_dict):
        id = job_in_dict.get('id', None)
        func = job_in_dict.get('func', None)
        args = job_in_dict.get('args', None)
        kwargs = job_in_dict.get('kwargs', None)
        trigger = job_in_dict.get('trigger', None)
        next_run_time = job_in_dict.get('next_run_time', None)
        job_key= job_in_dict.get('job_key', None)
        need_filter = job_in_dict.get('need_filter', False)
        replace_exist = job_in_dict.get('replace_exist', False)
        is_success = job_in_dict.get('is_success', None)
        details = job_in_dict.get('details', None)
        ref_to_func = None
        if isinstance(func, six.string_types):
            ref_to_func = func
        elif callable(func):
            ref_to_func = obj_to_ref(func)
        if trigger and not isinstance(trigger, BaseTrigger):
            raise WrongType
        if trigger:
            next_run_time = next_run_time or trigger.get_next_trigger_time(None)

        self.args = tuple(args) if args is not None else ()
        self.kwargs = dict(kwargs) if kwargs is not None else {}
        self.trigger = trigger
        self.next_run_time = next_run_time
        self.id = id or uuid4().hex
        self.__func = ref_to_func
        self.job_key = job_key
        self.need_filter = need_filter
        self.replace_exist = replace_exist
        self.is_success = is_success
        self.details = details

    def serialize(self):
        """
            dict representation of job
            :return:
        """
        job_in_dict = {
            'id': self.id,
            'func': self.__func,
            'trigger': self.trigger,
            'next_run_time': self.next_run_time,
            'args': self.args,
            'kwargs':self.kwargs,
            'job_key': self.job_key,
            'need_filter': self.need_filter,
            'replace_exist': self.replace_exist,
            'is_success': self.is_success,
            'details': self.details
        }
        return pickle.dumps(job_in_dict, pickle.HIGHEST_PROTOCOL)

    def check(self):
        check_callable_args(self.func, self.args, self.kwargs)

    @property
    def func(self):
        return ref_to_obj(self.__func)

    @property
    def filter_key(self):
        return "%s:filter" % self.job_key

    @classmethod
    def deserialize(cls, serialization):
        job_in_dict = pickle.loads(serialization)
        return cls(**job_in_dict)

    @classmethod
    def get_serial_run_times(cls, job, now):
        run_times = []
        next_run_time = job.next_run_time
        while next_run_time and next_run_time <= now:
            run_times.append(next_run_time)
            next_run_time = job.trigger.get_next_trigger_time(next_run_time, now)

        return run_times

    @classmethod
    def get_next_trigger_time(cls, job, run_time):
        return job.trigger.get_next_trigger_time(run_time)
