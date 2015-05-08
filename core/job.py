__author__ = 'Masutangu'
from core.utils import ref_to_obj, obj_to_ref, check_callable_args
try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle


class Job(object):

    def __init__(self, **job_info):
        func = job_info.pop('func')
        self.args = job_info.pop('args')
        self.kwargs = job_info.pop('kwargs')
        self.trigger = job_info.pop('trigger')


        ref_to_func = None
        if isinstance(func, str):
            ref_to_func = func
            func = ref_to_obj(func)
        elif callable(func):
            ref_to_func = obj_to_ref(func)

        check_callable_args(func, self.args, self.kwargs)

        self.ref_to_func = ref_to_func
        self.func = func

    def serialize(self):
        """
            dict representation of job
        :return:
        """
        job_in_dict = {
            'func': self.ref_to_func,
            'args': self.args,
            'kwargs':self.kwargs
        }
        return pickle.dumps(job_in_dict)

    @classmethod
    def deserialize(cls, serialization):
        job_in_dict = pickle.loads(serialization)
        return cls(**job_in_dict)






