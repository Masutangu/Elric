__author__ = 'moxu'
"""
    copy from apscheduler.util.py
"""

import six
from inspect import isfunction, ismethod, getargspec

try:
    from inspect import signature
except ImportError:  # pragma: nocover
    try:
        from funcsigs import signature
    except ImportError:
        signature = None



def get_callable_name(func):
    """
    Returns the best available display name for the given function/callable.

    :rtype: str
    """

    # the easy case (on Python 3.3+)
    if hasattr(func, '__qualname__'):
        return func.__qualname__

    # class methods, bound and unbound methods
    f_self = getattr(func, '__self__', None) or getattr(func, 'im_self', None)
    if f_self and hasattr(func, '__name__'):
        f_class = f_self if isinstance(f_self, type) else f_self.__class__
    else:
        f_class = getattr(func, 'im_class', None)

    if f_class and hasattr(func, '__name__'):
        return '%s.%s' % (f_class.__name__, func.__name__)

    # class or class instance
    if hasattr(func, '__call__'):
        # class
        if hasattr(func, '__name__'):
            return func.__name__

        # instance of a class with a __call__ method
        return func.__class__.__name__

    raise TypeError('Unable to determine a name for %r -- maybe it is not a callable?' % func)


def obj_to_ref(obj):
    """
    Returns the path to the given object.

    :rtype: str
    """

    try:
        ref = '%s:%s' % (obj.__module__, get_callable_name(obj))
        obj2 = ref_to_obj(ref)
        if obj != obj2:
            raise ValueError
    except Exception:
        raise ValueError('Cannot determine the reference to %r' % obj)

    return ref


def ref_to_obj(ref):
    """
    Returns the object pointed to by ``ref``.

    :type ref: str
    """

    if not isinstance(ref, six.string_types):
        raise TypeError('References must be strings')
    if ':' not in ref:
        raise ValueError('Invalid reference')

    modulename, rest = ref.split(':', 1)
    try:
        obj = __import__(modulename)
    except ImportError:
        raise LookupError('Error resolving reference %s: could not import module' % ref)

    try:
        for name in modulename.split('.')[1:] + rest.split('.'):
            obj = getattr(obj, name)
        return obj
    except Exception:
        raise LookupError('Error resolving reference %s: error looking up object' % ref)

def check_callable_args(func, args, kwargs):
    """
    Ensures that the given callable can be called with the given arguments.

    :type args: tuple
    :type kwargs: dict
    """

    pos_kwargs_conflicts = []  # parameters that have a match in both args and kwargs
    positional_only_kwargs = []  # positional-only parameters that have a match in kwargs
    unsatisfied_args = []  # parameters in signature that don't have a match in args or kwargs
    unsatisfied_kwargs = []  # keyword-only arguments that don't have a match in kwargs
    unmatched_args = list(args)  # args that didn't match any of the parameters in the signature
    unmatched_kwargs = list(kwargs)  # kwargs that didn't match any of the parameters in the signature
    has_varargs = has_var_kwargs = False  # indicates if the signature defines *args and **kwargs respectively

    if signature:
        try:
            sig = signature(func)
        except ValueError:
            return  # signature() doesn't work against every kind of callable

        for param in six.itervalues(sig.parameters):
            if param.kind == param.POSITIONAL_OR_KEYWORD:
                if param.name in unmatched_kwargs and unmatched_args:
                    pos_kwargs_conflicts.append(param.name)
                elif unmatched_args:
                    del unmatched_args[0]
                elif param.name in unmatched_kwargs:
                    unmatched_kwargs.remove(param.name)
                elif param.default is param.empty:
                    unsatisfied_args.append(param.name)
            elif param.kind == param.POSITIONAL_ONLY:
                if unmatched_args:
                    del unmatched_args[0]
                elif param.name in unmatched_kwargs:
                    unmatched_kwargs.remove(param.name)
                    positional_only_kwargs.append(param.name)
                elif param.default is param.empty:
                    unsatisfied_args.append(param.name)
            elif param.kind == param.KEYWORD_ONLY:
                if param.name in unmatched_kwargs:
                    unmatched_kwargs.remove(param.name)
                elif param.default is param.empty:
                    unsatisfied_kwargs.append(param.name)
            elif param.kind == param.VAR_POSITIONAL:
                has_varargs = True
            elif param.kind == param.VAR_KEYWORD:
                has_var_kwargs = True
    else:
        if not isfunction(func) and not ismethod(func) and hasattr(func, '__call__'):
            func = func.__call__

        try:
            argspec = getargspec(func)
        except TypeError:
            return  # getargspec() doesn't work certain callables

        argspec_args = argspec.args if not ismethod(func) else argspec.args[1:]
        arg_defaults = dict(zip(reversed(argspec_args), argspec.defaults or ()))
        has_varargs = bool(argspec.varargs)
        has_var_kwargs = bool(argspec.keywords)
        for arg in argspec_args:
            if arg in unmatched_kwargs and unmatched_args:
                pos_kwargs_conflicts.append(arg)
            elif unmatched_args:
                del unmatched_args[0]
            elif arg in unmatched_kwargs:
                unmatched_kwargs.remove(arg)
            elif arg not in arg_defaults:
                unsatisfied_args.append(arg)

    # Make sure there are no conflicts between args and kwargs
    if pos_kwargs_conflicts:
        raise ValueError('The following arguments are supplied in both args and kwargs: %s' %
                         ', '.join(pos_kwargs_conflicts))

    # Check if keyword arguments are being fed to positional-only parameters
    if positional_only_kwargs:
        raise ValueError('The following arguments cannot be given as keyword arguments: %s' %
                         ', '.join(positional_only_kwargs))

    # Check that the number of positional arguments minus the number of matched kwargs matches the argspec
    if unsatisfied_args:
        raise ValueError('The following arguments have not been supplied: %s' % ', '.join(unsatisfied_args))

    # Check that all keyword-only arguments have been supplied
    if unsatisfied_kwargs:
        raise ValueError('The following keyword-only arguments have not been supplied in kwargs: %s' %
                         ', '.join(unsatisfied_kwargs))

    # Check that the callable can accept the given number of positional arguments
    if not has_varargs and unmatched_args:
        raise ValueError('The list of positional arguments is longer than the target callable can handle '
                         '(allowed: %d, given in args: %d)' % (len(args) - len(unmatched_args), len(args)))

    # Check that the callable can accept the given keyword arguments
    if not has_var_kwargs and unmatched_kwargs:
        raise ValueError('The target callable does not accept the following keyword arguments: %s' %
                         ', '.join(unmatched_kwargs))
