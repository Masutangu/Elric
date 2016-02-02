# -*- coding: utf-8 -*-
from __future__ import absolute_import

from jobstore.base import BaseJobStore
from pymongo import MongoClient, errors
from core.utils import datetime_to_utc_timestamp, utc_timestamp_to_datetime
import pymongo
from core.exceptions import JobAlreadyExist, JobDoesNotExist
import datetime


class MongoJobStore(BaseJobStore):

    def __init__(self, context, url=None):
        BaseJobStore.__init__(self, context)
        self.client = MongoClient(url)
        self.db = self.client['elric']

    def add_job(self, job_id, job_key, next_run_time, serialized_job):
        """
            add job
            :type job_id: str
            :type job_key: str
            :type next_run_time: datetime.datetime
            :type serialized_job: str or xmlrpclib.Binary
        """
        next_timestamp = datetime_to_utc_timestamp(next_run_time)
        try:
            self.db.elric_jobs.insert_one(
                {
                    "serialized_job": serialized_job.decode("raw_unicode_escape"),
                    "job_key": job_key,
                    "next_timestamp": next_timestamp,
                    "_id": job_id,
                }
            )
        except pymongo.errors.DuplicateKeyError:
            raise JobAlreadyExist("add job failed! job [%s] has already exist" % job_id)

    def update_job(self, job_id, job_key=None, next_run_time=None, serialized_job=None):
        """
            update job
            :type job_id: str
            :type job_key: str
            :type next_run_time: datetime.datetime
            :type serialized_job: str or xmlrpclib.Binary
        """
        update_job_info = {}
        if job_key is not None:
            update_job_info['job_key'] = job_key
        if serialized_job is not None:
            update_job_info['serialized_job'] = serialized_job.decode("raw_unicode_escape")
        update_job_info['next_timestamp'] = datetime_to_utc_timestamp(next_run_time)
        result = self.db.elric_jobs.update_one(
            {"_id": job_id},
            {
                "$set": update_job_info,
                "$currentDate": {"lastModified": True}
            }
        )
        if result.matched_count == 0:
            raise JobDoesNotExist("update job failed! job [%s] does not exist" % job_id)

    def remove_job(self, job_id):
        """
            remove job
            :type job_id: str
        """
        result = self.db.elric_jobs.delete_one({"_id": job_id})
        if result.deleted_count == 0:
            raise JobDoesNotExist("remove job failed! job [%s] does not exist" % job_id)

    def save_execute_record(self, job_id, is_success, details):
        execute_info = {"is_success": is_success, "details": details,
                        "report_timestamp": datetime.datetime.now()}
        self.db.elric_execute_records.update(
            {"_id": job_id},
            {
                "$push": {
                    "execute_records": {
                        "$each": [execute_info, ],
                        "$sort": {"report_timestamp": 1},
                        "$slice": -3
                    }
                }
            },
            upsert=True,
        )

    def get_due_jobs(self, now):
        """
            Get due jobs.
            :type now: datetime.datetime
        """
        curr_timestamp = datetime_to_utc_timestamp(now)
        due_jobs = []
        cursor = self.db.elric_jobs.find({"next_timestamp": {"$lt": curr_timestamp}})
        for job in cursor:
            due_jobs.append((job["_id"], job['job_key'], job['serialized_job'].encode("raw_unicode_escape")))
        return due_jobs

    def get_closest_run_time(self):
        cursor = self.db.elric_jobs.find({}, {'next_timestamp': True, '_id': False}).sort([ #projection
            ("next_timestamp", pymongo.ASCENDING),
        ]).limit(1)
        if cursor.count() > 0:
            return utc_timestamp_to_datetime(cursor[0]['next_timestamp'])
