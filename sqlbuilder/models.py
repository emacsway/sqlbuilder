from django.conf import settings
from django.db.models import Model

import smartsql
import sqlobject

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 'ss')
SQLOBJECT_ALIAS = getattr(settings, 'SQLBUILDER_SQLOBJECT_ALIAS', 'so')


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


@classproperty
def ss(cls):
    return getattr(
        smartsql.Table,
        cls._meta.db_table
    )

setattr(Model, SMARTSQL_ALIAS, ss)


@classproperty
def so(cls):
    return getattr(
        sqlobject.table,
        cls._meta.db_table
    )

setattr(Model, SQLOBJECT_ALIAS, so)
