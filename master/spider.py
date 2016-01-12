# -*- coding:utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from master.rqueue import RQMaster
from dupefilter.memoryfilter import MemoryFilter
from core.exceptions import JobAlreadyExist
from xmlrpclib import Binary
from core.job import Job
from threading import RLock
import threading
import cPickle
from time import sleep
import copy
import os


class Spider(RQMaster):
    def __init__(self, timezone=None):
        RQMaster.__init__(self, timezone)
        self.filter_list = {}
        self.filter_lock = RLock()
        self.rpc_server.register_function(self.finish_job, 'finish_job')
        self.read_filter_file()
        self.start_serialize_data()

    def read_filter_file(self):
        try:
            f = open("elric_filter.dump", "rb")
            self.filter_list = cPickle.load(f)
            f.close()
        except IOError as e:
            self.log.warn("open elric_filter.dump failed, exception=%s" % e)

    def submit_job(self, serialized_job, job_key, job_id, replace_exist):
        def exist(key, value):
            with self.filter_lock:
                try:
                    return self.filter_list[key].exist(value)
                except KeyError:
                    self.filter_list[key] = MemoryFilter()
                    return self.filter_list[key].exist(value)

        self.log.debug("client call submit job [%s]" % job_id)

        if isinstance(serialized_job, Binary):
            serialized_job = serialized_job.data

        job_in_dict = Job.deserialize_to_dict(serialized_job)
        filter_key = job_in_dict['filter_key']
        filter_value = job_in_dict['filter_value']

        if filter_key and filter_value:
            if exist(filter_key, filter_value):
                self.log.debug("job [%s] has been filter..." % job_id)
                return False

        if not job_in_dict['trigger']:
            self._enqueue_job(job_key, serialized_job)
        else:
            with self.jobstore_lock:
                try:
                    self.jobstore.add_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                except JobAlreadyExist:
                    if replace_exist:
                        self.jobstore.update_job(job_id, job_key, job_in_dict['next_run_time'], serialized_job)
                    else:
                        self.log.warn('job [%s] already exist' % job_id)
            self.wake_up()

        return True

    def finish_job(self, job_id, key, value):
        self.log.debug("job_id [%s] finish" % job_id)
        if key and value:
            with self.filter_lock:
                try:
                    self.filter_list[key].add(value)
                except KeyError:
                    self.filter_list[key] = MemoryFilter()
                    return self.filter_list[key].add(value)

    def serialize_data(self):
        """
            dump filter_list to file every five minutes
        """
        while True:
            self.log.debug("start to serialize data..")
            f = open("elric_filter.dump.bk", "wb")
            current_filter_list = copy.deepcopy(self.filter_list)
            cPickle.dump(current_filter_list, f)
            f.close()
            os.rename("elric_filter.dump.bk", "elric_filter.dump")
            sleep(60)

    def start_serialize_data(self):
        """
            Start filter data serialization thread
        """
        self.log.debug('start serialize_data thread...')
        thd = threading.Thread(target=self.serialize_data)
        thd.setDaemon(True)
        thd.start()
