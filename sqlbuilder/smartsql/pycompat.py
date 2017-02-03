from __future__ import absolute_import

__all__ = ('str', 'string_types', 'integer_types')

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

except NameError:
    str = str
    string_types = (str,)
    integer_types = (int,)
