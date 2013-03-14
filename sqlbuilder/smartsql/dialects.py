from __future__ import absolute_import, unicode_literals
from . import Condition, sql_dialects, sqlrepr

TRANSLATION_MAP = {
    'sqlite': {
        'LIKE': 'GLOB',
        'ILIKE': 'LIKE',
    },
    'mysql': {
        'LIKE': 'LIKE BINARY',
        'ILIKE': 'LIKE',
    }
}


@sql_dialects.register('sqlite', Condition)
@sql_dialects.register('mysql', Condition)
def condition_sqlrepl(self, dialect):
    """Translates operators for sqlbuilder.smartsql.Condition"""
    s1 = sqlrepr(self._expr1, dialect)
    s2 = sqlrepr(self._expr2, dialect)
    op = TRANSLATION_MAP.get(dialect, {}).get(self._op, self._op)
    if not s1:
        return s2
    if not s2:
        return s1
    return "{0} {1} {2}".format(s1, op, s2)
