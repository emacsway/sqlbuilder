from __future__ import absolute_import, unicode_literals
from . import (Condition, Concat, ExprList, Name, sql_dialects, sqlrepr)

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
def condition_sqlrepr(self, dialect):
    """Translates operators for sqlbuilder.smartsql.Condition"""
    op = TRANSLATION_MAP.get(dialect, {}).get(self._sql, self._sql)
    return "{0} {1} {2}".format(sqlrepr(self._left, dialect), op, sqlrepr(self._right, dialect))


@sql_dialects.register('sqlite', Name)
@sql_dialects.register('mysql', Name)
def name_sqlrepr(self, dialect):
    return self._sqlrepr_base('`', dialect)


@sql_dialects.register('mysql', Concat)
def name_sqlrepr(self, dialect):
    value = sqlrepr(self.join(', '), dialect, ExprList)
    if self._ws:
        return "CONCAT_WS({0}, {1})".format(sqlrepr(self._ws, dialect), value)
    return "CONCAT({0})".format(value)
