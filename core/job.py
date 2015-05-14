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
        ref_to_func = None
        if isinstance(func, six.string_types):
            ref_to_func = func
            func = ref_to_obj(func)
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
        self.func = func
        self.ref_to_func = ref_to_func

        check_callable_args(self.func, self.args, self.kwargs)

    def serialize(self):
        """
            dict representation of job
            :return:
        """
        job_in_dict = {
            'id': self.id,
            'func': self.ref_to_func,
            'trigger': self.trigger,
            'next_run_time': self.next_run_time,
            'args': self.args,
            'kwargs':self.kwargs
        }
        return pickle.dumps(job_in_dict, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, serialization):
        job_in_dict = cls.deserialize_to_dict(serialization)
        return cls(**job_in_dict)

    @classmethod
    def deserialize_to_dict(cls, serialization):
        return pickle.loads(serialization)

    @classmethod
    def dict_to_serialization(cls, job_in_dict):
        return pickle.dumps(job_in_dict, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def get_serial_run_times(cls, job_in_dict, now):
        run_times = []
        next_run_time = job_in_dict['next_run_time']
        while next_run_time and next_run_time <= now:
            run_times.append(next_run_time)
            next_run_time = job_in_dict['trigger'].get_next_trigger_time(next_run_time, now)

        return run_times

    @classmethod
    def get_next_trigger_time(cls, job_in_dict, run_time):
        return job_in_dict['trigger'].get_next_trigger_time(run_time)

    def update(self, **new_info):
        if 'args' in new_info:
            self.args = new_info.pop('args')
        if 'kwargs' in new_info:
            self.kwargs = new_info.pop('kwargs')
        if 'func' in new_info:
            func = new_info.pop('func')
            ref_to_func = None
            if isinstance(func, str):
                ref_to_func = func
                func = ref_to_obj(func)
            elif callable(func):
                ref_to_func = obj_to_ref(func)

            check_callable_args(func, self.args, self.kwargs)

            self.ref_to_func = ref_to_func
            self.func = func

        if 'trigger' in new_info:
            self.trigger = new_info.pop('trigger')

        if 'next_run_time' in new_info:
            self.next_run_time = new_info.pop('next_run_time')

        #TODO: messy code.. need clean up
        if self.trigger:
            # timezone may have bug here
            self.next_run_time = self.next_run_time or self.trigger.get_next_trigger_time(None)






