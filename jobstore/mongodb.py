# -*- coding: utf-8 -*-
from __future__ import absolute_import

from jobstore.base import BaseJobStore
from pymongo import MongoClient, errors
from core.utils import datetime_to_utc_timestamp, utc_timestamp_to_datetime
import pymongo
from core.exceptions import JobAlreadyExist, JobDoesNotExist
import datetime


class MongoJobStore(BaseJobStore):
    def __init__(self, context, **config):
        BaseJobStore.__init__(self, context)
        self.client = MongoClient(**config['server'])
        self.db = self.client['elric']
        self.max_preserve_records = config['maximum_records']

    def add_job(self, job):
        """
            add job
            :type job: Job
        """
        next_timestamp = datetime_to_utc_timestamp(job.next_run_time)
        try:
            self.db.elric_jobs.insert_one(
                {
                    "serialized_job": job.serialize().decode("raw_unicode_escape"),
                    "job_key": job.job_key,
                    "next_timestamp": next_timestamp,
                    "_id": job.id,
                }
            )
        except pymongo.errors.DuplicateKeyError:
            raise JobAlreadyExist("add job failed! job [%s] has already exist" % job.id)

    def update_job(self, job):
        """
            update job
            :type job: Job
        """
        update_job_info = {}
        if job.job_key is not None:
            update_job_info['job_key'] = job.job_key

        update_job_info['serialized_job'] = job.serialize().decode("raw_unicode_escape")
        update_job_info['next_timestamp'] = datetime_to_utc_timestamp(job.next_run_time)
        result = self.db.elric_jobs.update_one(
            {"_id": job.id},
            {
                "$set": update_job_info,
                "$currentDate": {"lastModified": True}
            }
        )
        if result.matched_count == 0:
            raise JobDoesNotExist("update job failed! job [%s] does not exist" % job.id)

    def remove_job(self, job):
        """
            remove job
            :type job: Job
        """
        result = self.db.elric_jobs.delete_one({"_id": job.id})
        if result.deleted_count == 0:
            raise JobDoesNotExist("remove job failed! job [%s] does not exist" % job.id)

    def save_execute_record(self, job):
        execute_info = {"is_success": job.is_success, "details": job.details,
                        "report_timestamp": datetime.datetime.now()}
        self.db.elric_execute_records.update(
            {"_id": job.id},
            {
                "$push": {
                    "execute_records": {
                        "$each": [execute_info, ],
                        "$sort": {"report_timestamp": 1},
                        "$slice": -self.max_preserve_records
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
