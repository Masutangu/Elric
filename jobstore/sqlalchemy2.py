# -*- coding: utf-8 -*-
from __future__ import (absolute_import, unicode_literals)

from jobstore.base import BaseJobStore
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine,  Column, Float, LargeBinary, String, Integer, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from core.utils import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from core.exceptions import JobAlreadyExist, JobDoesNotExist



Base = declarative_base()
Session = sessionmaker()


class ElricJob(Base):
    __tablename__ = 'elric_jobs'

    id = Column(String, primary_key=True)
    job_key = Column(String)
    next_run_time = Column(Float(25), index=True, nullable=False)
    serialized_job = Column(LargeBinary, nullable=False)
    status = Column(Integer, default=0)
    """
    不知道为啥加上__init__函数之后，job_key赋值就变成tuple..
    def __init__(self, id, job_key, next_run_time, serialized_job):
        self.id = id
        self.job_key = job_key,
        self.next_run_time = next_run_time
        self.serialized_job = serialized_job
    """

    def __repr__(self):
        return "<Elric('%s', '%s', '%f')>" % (self.id, self.key, self.next_run_time)


class SQLAlchemyJobStore(BaseJobStore):
    def __init__(self, url):
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        Session.configure(bind=self.engine)
        self.session = Session()

    def add_job(self, job_id, job_key, next_run_time, serialized_job):
        timestamp = datetime_to_utc_timestamp(next_run_time)
        try:
            job = ElricJob(id=job_id, job_key=job_key,
                next_run_time=datetime_to_utc_timestamp(next_run_time),
                serialized_job=serialized_job)

            self.session.add(job)
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            raise JobAlreadyExist

    #TODO: raise error when job_id does not exist
    def update_job(self, job_id, job_key=None, next_run_time=None, serialized_job=None, status=None):
        new_value = {}
        if job_key:
            new_value['job_key'] = job_key
        if next_run_time:
            new_value['next_run_time'] = next_run_time
        if serialized_job:
            new_value['serialized_job'] = serialized_job
        if status or status == 0:
            new_value['status'] = status
        self.session.query(ElricJob).filter_by(id=job_id).update(new_value)
        self.session.commit()

    def remove_job(self, job_id):
        self.session.query(ElricJob).filter(ElricJob.id == job_id).delete(synchronize_session=False)
        self.session.commit()

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self.session.query(ElricJob.id, ElricJob.job_key, ElricJob.serialized_job).filter(
                and_(ElricJob.next_run_time <= timestamp, ElricJob.status == 0)).all()

    # get the closest run time
    def get_closest_run_time(self):
        query_result = self.session.query(ElricJob.next_run_time).filter(ElricJob.status == 0).order_by(
                        ElricJob.next_run_time).first()
        if query_result:
            return utc_timestamp_to_datetime(query_result[0])




