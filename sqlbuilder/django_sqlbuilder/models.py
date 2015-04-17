from __future__ import absolute_import, unicode_literals
import copy
import collections
from django.conf import settings
from django.db import connections
from django.db.models import Model

from .. import smartsql
from ..smartsql.compilers import mysql
from ..smartsql.compilers import sqlite
from .signals import field_conversion

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

cr = copy.copy(smartsql.cr)


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class Result(smartsql.Result):

    _cache = None
    _using = 'default'
    model = None

    def __init__(self, model, compile):
        Result.__init__(self, compile)
        self.model = model
        self._using = self.model.objects.db
        self.set_compiler()

    def fill_cache(self):
        if self._cache is None:
            self._cache = list(self.iterator())
        return self

    def __len__(self):
        """Returns length or list."""
        self.fill_cache()
        return len(self._cache)

    def iterator(self):
        return self.execute(self)

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        """Returns sliced self or item."""
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self._query = self._query.__getitem__(key, True)
            return list(self)[0]
        return self._query.__getitem__(key, True)

    def using(self, alias=None):
        if alias is None:
            return self._using
        self._using = alias
        self.set_compiler()
        return self

    def set_compiler(self):
        engine = connections.databases[self._using]['ENGINE'].rsplit('.')[-1]
        self.compile = SMARTSQL_COMPILERS[engine]
        return self

    def execute(self, expr):
        """Implementation of query execution"""
        if isinstance(expr, smartsql.SelectCount):
            pass
        elif isinstance(expr, smartsql.Query):
            return self.model.objects.raw(*self.compile(self)).using(self._using)
        return self._execute(*self.compile(self))

    def _execute(self, sql, params):
        cursor = connections[self._using].cursor()
        cursor.execute(sql, params)
        return cursor

    def __call__(self, expr=None):
        """Result"""
        expr = self if expr is None else expr
        if isinstance(expr, smartsql.SelectCount):
            return self.execute(expr).fetchone()[0]
        elif isinstance(expr, smartsql.Query):
            return self
        return self.execute(expr)

    def clone(self):
        c = smartsql.Result.clone(self)
        c._cache = None
        return c


@cr('Query')
class QS(smartsql.QS):
    """Query Set adapted for Django."""

    _cache = None
    _using = 'default'
    model = None

    def __init__(self, tables=None):
        super(QS, self).__init__(tables=tables)
        if isinstance(tables, (Table, TableAlias)):
            self.model = tables.model
            self._using = self.model.objects.db
        self.set_compiler()

    def clone(self, *attrs):
        self = smartsql.QS.clone(self, *attrs)
        self._cache = None
        return self

    def fill_cache(self):
        if self._cache is None:
            self._cache = list(self.iterator())
        return self

    def __len__(self):
        """Returns length or list."""
        self.fill_cache()
        return len(self._cache)

    def count(self):
        """Returns length or list."""
        if self._cache is not None:
            return len(self._cache)
        return super(QS, self).count()

    def iterator(self):
        return self.execute(self)

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        """Returns sliced self or item."""
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self = super(QS, self).__getitem__(key)
            return list(self)[0]
        return super(QS, self).__getitem__(key)

    def using(self, alias=None):
        if alias is None:
            return self._using
        self = self.clone()
        self._using = alias
        self.set_compiler()
        return self

    def set_compiler(self):
        engine = connections.databases[self._using]['ENGINE'].rsplit('.')[-1]
        self.compile = SMARTSQL_COMPILERS[engine]
        return self

    def execute(self, expr):
        """Implementation of query execution"""
        if isinstance(expr, smartsql.SelectCount):
            pass
        elif isinstance(expr, smartsql.Query):
            return self.model.objects.raw(*self.compile(self)).using(self._using)
        return self._execute(*self.compile(self))

    def _execute(self, sql, params):
        cursor = connections[self._using].cursor()
        cursor.execute(sql, params)
        return cursor

    def result(self, expr=None):
        """Result"""
        expr = self if expr is None else expr
        if isinstance(expr, smartsql.SelectCount):
            return self.execute(expr).fetchone()[0]
        elif isinstance(expr, smartsql.Query):
            return self
        return self.execute(expr)


@cr
class Set(smartsql.Set, QS):
    """Union query class"""
    def __init__(self, exprs, *a, **kw):
        super(Set, self).__init__(exprs, *a, **kw)
        self.model = exprs[0].model
        self._using = exprs[0]._using


@cr
class Table(smartsql.Table):
    """Table class for Django model"""

    def __init__(self, model, qs=None, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(model._meta.db_table, *args, **kwargs)
        self.model = model
        self._qs = qs

    def _get_qs(self):
        if isinstance(self._qs, collections.Callable):
            self._qs = self._qs(self)
        elif self._qs is None:
            self._qs = QS(self).fields(self.get_fields())
        return self._qs.clone()

    def _set_qs(self, val):
        self._qs = val

    qs = property(_get_qs, _set_qs)

    def get_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self
        result = []
        for f in self.model._meta.local_fields:
            if f.column:
                result.append(smartsql.Field(f.column, prefix))
        return result

    def __getattr__(self, name):
        """Added some django specific functional."""
        m = self.model
        if name[0] == '_':
            raise AttributeError
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
        elif parts[0] in m._meta.get_all_field_names():
            parts[0] = m._meta.get_field(parts[0]).column

        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


@cr
class TableAlias(smartsql.TableAlias, Table):
    """Table alias class"""
    @property
    def model(self):
        return getattr(self._table, 'model', None)  # Can be subquery


@classproperty
def s(cls):
    a = '_{0}'.format(SMARTSQL_ALIAS)
    if a not in cls.__dict__:
        setattr(cls, a, Table(cls))
    return getattr(cls, a)

setattr(Model, SMARTSQL_ALIAS, s)
