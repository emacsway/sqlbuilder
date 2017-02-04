from __future__ import absolute_import, unicode_literals
import copy
import collections
from itertools import chain
from django.conf import settings
from django.db import connections
from django.db.models import Model

from sqlbuilder import smartsql
from sqlbuilder.smartsql.dialects import mysql
from django_sqlbuilder.dialects import sqlite
from django_sqlbuilder.signals import field_conversion

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 's')
SMARTSQL_COMPILERS = {
    'sqlite3': sqlite.compile,
    'mysql': mysql.compile,
    'postgresql': smartsql.compile,
    'postgresql_psycopg2': smartsql.compile,
    'postgis': smartsql.compile,
}

factory = copy.copy(smartsql.factory)


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class Result(smartsql.Result):

    _cache = None
    _using = 'default'
    _model = None

    def __init__(self, model):
        self._model = model
        self._using = self._model.objects.db
        self.set_compiler()

    def __len__(self):
        self.fill_cache()
        return len(self._cache)

    def __iter__(self):
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self._query = super(Result, self).__getitem__(key)
            return list(self)[0]
        return super(Result, self).__getitem__(key)

    def execute(self):
        cursor = connections[self._using].cursor()
        cursor.execute(*self.compile(self._query))
        return cursor

    insert = update = delete = execute

    def select(self):
        return self

    def count(self):
        if self._cache is not None:
            return len(self._cache)
        return self.execute().fetchone()[0]

    def clone(self):
        c = smartsql.Result.clone(self)
        c._cache = None
        return c

    def using(self, alias=None):
        if alias is None:
            return self._using
        self._using = alias
        self.set_compiler()
        return self._query

    def set_compiler(self):
        engine = connections.databases[self._using]['ENGINE'].rsplit('.')[-1]
        self.compile = SMARTSQL_COMPILERS[engine]

    def fill_cache(self):
        if self._cache is None:
            self._cache = list(self.iterator())

    def iterator(self):
        return self._model.objects.raw(*self.compile(self._query)).using(self._using)


@factory.register
class Table(smartsql.Table):
    """Table class for Django model"""

    def __init__(self, model, q=None, *args, **kwargs):
        super(Table, self).__init__(model._meta.db_table, *args, **kwargs)
        self._model = model
        self._q = q

    def _get_q(self):
        if isinstance(self._q, collections.Callable):
            self._q = self._q(self)
        elif self._q is None:
            self._q = smartsql.factory.get(self).Query(self, result=Result(self._model)).fields(self.get_fields())
        return self._q.clone()

    def _set_q(self, val):
        self._q = val

    qs = property(_get_q, _set_q)
    q = property(_get_q, _set_q)

    def get_fields(self, prefix=None):
        if prefix is None:
            prefix = self
        result = []
        for f in self._model._meta.local_fields:
            if f.column:
                result.append(smartsql.Field(f.column, prefix))
        return result

    def get_field(self, name):
        m = self._model
        parts = name.split(smartsql.LOOKUP_SEP, 1)

        # Why do not to use responses, what returned by Signal.send()?
        # In current way we can attach additional information to result
        # mutable variable and pass it between signal's handlers.
        result = {'field': parts[0], }
        field_conversion.send(sender=self, result=result, field=parts[0], model=m)
        parts[0] = result['field']

        # django-multilingual-ext support
        if 'modeltranslation' in settings.INSTALLED_APPS:
            from modeltranslation.translator import translator, NotRegistered
            from modeltranslation.utils import get_language, build_localized_fieldname
        else:
            translator = None
        if translator:
            try:
                trans_opts = translator.get_options_for_model(m)
                if parts[0] in trans_opts.fields:
                    parts[0] = build_localized_fieldname(parts[0], get_language())
            except NotRegistered:
                pass
        if hasattr(m.objects, 'localize_fieldname'):
            parts[0] = m.objects.localize_fieldname(parts[0])

        # model attributes support
        if parts[0] == 'pk':
            parts[0] = m._meta.pk.column
        elif parts[0] in get_all_field_names(m._meta):
            parts[0] = m._meta.get_field(parts[0]).column

        return super(Table, self).get_field(smartsql.LOOKUP_SEP.join(parts))


@factory.register
class TableAlias(smartsql.TableAlias, Table):
    @property
    def _model(self):
        return getattr(self._table, '_model', None)  # Can be subquery


def get_all_field_names(opts):
    try:
        return list(set(chain.from_iterable(
            (field.name, field.attname) if hasattr(field, 'attname') else (field.name,)
            for field in opts.get_fields()
            # For complete backwards compatibility, you may want to exclude
            # GenericForeignKey from the results.
            if not (field.many_to_one and field.related_model is None)
        )))
    except AttributeError:
        return opts.get_all_field_names()


@classproperty
def s(cls):
    a = '_{0}'.format(SMARTSQL_ALIAS)
    if a not in cls.__dict__:
        setattr(cls, a, factory.Table(cls))
    return getattr(cls, a)

setattr(Model, SMARTSQL_ALIAS, s)
