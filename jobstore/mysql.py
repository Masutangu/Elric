# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from jobstore.base import BaseJobStore
from core.utils import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from core.exceptions import JobAlreadyExist, JobDoesNotExist
from MySQLdb import OperationalError

import MySQLdb


class MySQLJobStore(BaseJobStore):

    def __init__(self, host='localhost', port=3306, db='elric', charset='utf8', user='', passwd=''):
        self.host = host
        self.port = port
        self.db = db
        self.charset = charset
        self.user = user
        self.passwd = passwd
        self.do_connect()

        create_table_sql = ("CREATE TABLE IF NOT EXISTS elric_jobs("
                            "job_id varchar(255) NOT NULL PRIMARY KEY,   "
                            "job_key varchar(255) NOT NULL,"
                            "next_run_time FLOAT NOT NULL,           "
                            "serialized_job BLOB NOT NULL,"
                            "status int NOT NULL DEFAULT 0,"
                            "INDEX(next_run_time)"
                            ");")

        print create_table_sql

        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def do_connect(self):
        self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                    db=self.db, charset=self.charset)
        self.cursor = self.conn.cursor()

    def add_job(self, job_id, job_key, next_run_time, serialized_job):
        add_job_sql = ("INSERT INTO elric_jobs(job_id, job_key, next_run_time, serialized_job, status) VALUES("
                       "'{job_id}', '{job_key}', {next_run_time}, '{serialized_job}', 0)").format(
                        job_id=job_id, job_key=job_key, next_run_time=next_run_time,
                        serialized_job=serialized_job)
        #print 'add_job_sql=', add_job_sql
        self.cursor.execute(add_job_sql)
        self.conn.commit()

    #TODO: raise error when job_id does not exist
    def update_job(self, job_id, job_key=None, next_run_time=None, serialized_job=None, status=None):
        update_job_sql = "UPDATE elric_jobs SET "
        set_values = []
        if job_key:
            set_values.append("job_key='{job_key}'".format(job_key=job_key))
        if serialized_job:
            set_values.append("serialized_job='{serialized_job}'".format(serialized_job=serialized_job))
        if next_run_time:
            set_values.append("next_run_time={next_run_time}".format(next_run_time=next_run_time))
        if status or status == 0:
            set_values.append("status={status}".format(status=status))

        if not set_values:
            return
        update_job_sql += (','.join(set_values))
        set_values_str = (','.join(set_values))
        print set_values_str
        update_job_sql += (" WHERE job_id='{job_id}'".format(job_id=job_id))

        print 'update_job_sql=', update_job_sql
        row_count = self.cursor.execute(update_job_sql)
        self.conn.commit()
        if row_count == 0:
            raise JobDoesNotExist

    def remove_job(self, job_id):
        delete_job_sql = ("DELETE FROM elric_jobs WHERE job_id='{job_id}'").format(
                           job_id=job_id)
        #print 'delete_job_sql=', delete_job_sql
        row_count = self.cursor.execute(delete_job_sql)
        self.conn.commit()
        if row_count == 0:
            raise JobDoesNotExist

    def get_due_jobs(self, now):
        curr_timestamp = datetime_to_utc_timestamp(now)
        get_due_jobs_sql = ("SELECT job_id, job_key, serialized_job FROM elric_jobs "
                            "WHERE next_run_time < {curr_timestamp} AND status=0").format(
                            curr_timestamp=curr_timestamp)
        print 'get_due_jobs_sql=', get_due_jobs_sql
        self.cursor.execute(get_due_jobs_sql)

        query_results = self.cursor.fetchall()
        for job in query_results:
            yield job


    # get the closest run time
    def get_closest_run_time(self):
        get_closest_run_time = "SELECT next_run_time FROM elric_jobs WHERE status=0 ORDER BY next_run_time LIMIT 1"
        self.cursor.execute(get_closest_run_time)
        query_result = self.cursor.fetchone()
        if query_result:
            return utc_timestamp_to_datetime(query_result[0])




