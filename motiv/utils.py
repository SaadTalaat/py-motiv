"""
Module:
    motiv.utils

Description:
    contains common used functions.
"""

from collections.abc import Iterable

def ensure_pair(instance, _type):
    if _type is None or isinstance(instance, _type):
        return
    elif isinstance(_type, Iterable):
        # _type argument might be a tuple of types.
        _types = ", ".join([t.__name__ for t in _type])
        raise TypeError(f"Expected {instance} to be of any of these types"\
                f"{_types}")
    else:
        raise TypeError(f"Expected {instance} to be of type {_type.__name__}")

def ensure_types(*args):
    """
    Arguments:
        - Must be of even length
        - consists on list of instances followed by
            probable type matches.
        example:
            ensure_types(instance, FooType, instance2, (BarType, BazType))
    """
    for i, arg in enumerate(args[::2]):
        ensure_pair(arg, args[i+1])

def signature(*type_args):
    def actual_decorator(fn):
        def wrapper_function(*args, **kwargs):
            for arg, type_arg in zip(args, type_args):
                ensure_pair(arg, type_arg)
            return fn(*args, **kwargs)
        return wrapper_function
    return actual_decorator
