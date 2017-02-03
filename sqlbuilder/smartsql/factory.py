from __future__ import absolute_import
from sqlbuilder.smartsql.pycompat import string_types

__all__ = ('Factory', 'factory',)


class Factory(object):

    def register(self, name_or_callable):
        name = name_or_callable if isinstance(name_or_callable, string_types) else name_or_callable.__name__

        def deco(callable_obj):

            def wrapped_obj(*a, **kw):
                instance = callable_obj(*a, **kw)
                instance.__factory__ = self
                return instance

            setattr(self, name, wrapped_obj)
            return callable_obj

        return deco if isinstance(name_or_callable, string_types) else deco(name_or_callable)

    @classmethod
    def get(cls, instance):
        try:
            return instance.__factory__
        except AttributeError:
            return cls.default()

    @staticmethod
    def default():
        cls = Factory
        if not hasattr(cls, '_default'):
            cls._default = cls()
        return cls._default

factory = Factory.default()
