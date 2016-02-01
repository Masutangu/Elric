# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from abc import ABCMeta, abstractmethod


class BaseJobStore(object):
    __metaclass__ = ABCMeta

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def add_job(self, job_id, job_key, next_run_time, serialized_job):
        raise NotImplementedError('subclasses of BaseJobStore must provide a add_job() method')

    @abstractmethod
    def update_job(self, job_id, job_key=None, next_run_time=None, serialized_job=None):
        raise NotImplementedError('subclasses of BaseJobStore must provide a update_job() method')

    @abstractmethod
    def remove_job(self, job_id):
        raise NotImplementedError('subclasses of BaseJobStore must provide a remove_job() method')

    @abstractmethod
    def update_execute_record(self, job_id, is_success, details):
        raise NotImplementedError('subclasses of BaseJobStore must provide a update_execute_record() method')

    @abstractmethod
    def get_due_jobs(self, now):
        raise NotImplementedError('subclasses of BaseJobStore must provide a get_due_jobs() method')

    @abstractmethod
    def get_closest_run_time(self):
        raise NotImplementedError('subclasses of BaseJobStore must provide a get_closest_run_time() method')
