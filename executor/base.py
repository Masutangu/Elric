__author__ = 'Masutangu'

import logging
import logging.handlers
from abc import ABCMeta, abstractmethod

executor_logger = logging.getLogger('executor')
executor_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler('executor.log', maxBytes=10000000, backupCount=5)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
executor_logger.addHandler(handler)


class BaseExecutor(object):
    __metaclass__ = ABCMeta

    def __init__(self, logger=None):
        self.log = logger or executor_logger
        #pass

    @abstractmethod
    def execute_job(self, job):
        raise NotImplementedError

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError
