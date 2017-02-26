from __future__ import absolute_import
import warnings
from functools import wraps

__all__ = ('Undef', 'UndefType', 'is_list', 'is_allowed_attr', 'opt_checker', 'same', 'warn', )


class UndefType(object):

    def __repr__(self):
        return "Undef"

    def __reduce__(self):
        return "Undef"


Undef = UndefType()


class AutoName(object):
    def __init__(self, prefix="_auto_"):
        self.__counter = 0
        self.prefix = prefix

    def __iter__(self):
        return self

    def __next__(self):
        self.__counter += 1
        return "{0}{1}".format(self.prefix, self.__counter)

    next = __next__


auto_name = AutoName()


def is_allowed_attr(instance, key):
    if key.startswith('__'):
        return False
    if key in dir(instance.__class__):  # type(instance)?
        # It's a descriptor, like 'sql' defined in slots
        return False
    return True


def is_list(value):
    return isinstance(value, (list, tuple))


def same(name):
    def f(self, *a, **kw):
        return getattr(self, name)(*a, **kw)
    return f


def opt_checker(k_list):
    def new_deco(f):
        @wraps(f)
        def new_func(self, *args, **opt):
            for k, v in list(opt.items()):
                if k not in k_list:
                    raise TypeError("Not implemented option: {0}".format(k))
            return f(self, *args, **opt)
        return new_func
    return new_deco


def warn(old, new, stacklevel=3):
    warnings.warn("{0} is deprecated. Use {1} instead".format(old, new), PendingDeprecationWarning, stacklevel=stacklevel)
