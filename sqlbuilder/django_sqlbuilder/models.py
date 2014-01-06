from __future__ import absolute_import, unicode_literals
import re
import collections
from django.conf import settings
from django.db import connections
from django.db.models import Model
from django.db.models.manager import Manager
from django.db.models.query import RawQuerySet

from .. import smartsql
from .signals import field_conversion

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 's')
SMARTSQL_DIALECTS = {
    'sqlite3': 'sqlite',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'postgresql_psycopg2': 'postgres',
    'postgis': 'postgres',
    'oracle': 'oracle',
}


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class QS(smartsql.QS):
    """Query Set adapted for Django."""

    _cache = None
    model = None

    def __init__(self, tables=None):
        super(QS, self).__init__(tables=tables)
        if isinstance(tables, (Table, TableAlias)):
            self.model = tables.model

    def clone(self):
        self = super(QS, self).clone()
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
        return self.order_by(reset=True).execute().count()

    def iterator(self):
        return self.execute().iterator()

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        """Returns sliced self or item."""
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self = self.clone()
            self = super(QS, self).__getitem__(key)
            return list(self)[0]
        return super(QS, self).__getitem__(key)

    def dialect(self):
        #engine = connection.settings_dict['ENGINE'].rsplit('.')[-1]
        engine = connections.databases[self.model.objects.db]['ENGINE'].rsplit('.')[-1]
        return SMARTSQL_DIALECTS[engine]

    def sqlrepr(self, expr=None):
        return smartsql.sqlrepr(expr or self, self.dialect())

    def sqlparams(self, expr=None):
        return smartsql.sqlparams(expr or self)

    def execute(self):
        """Implementation of query execution"""
        return self.model.objects.raw(
            self.sqlrepr(),
            self.sqlparams()
        )

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()

    def as_union(self):
        return UnionQuerySet(self)


class UnionQuerySet(smartsql.UnionQuerySet, QS):
    """Union query class"""
    def __init__(self, qs):
        super(UnionQuerySet, self).__init__(qs)
        self.model = qs.model


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

    def as_(self, alias):
        return TableAlias(alias, self)


class TableAlias(smartsql.TableAlias, Table):
    """Table alias class"""
    @property
    def model(self):
        return self._table.model


@classproperty
def s(cls):
    a = '_{0}'.format(SMARTSQL_ALIAS)
    if a not in cls.__dict__:
        setattr(cls, a, Table(cls))
    return getattr(cls, a)

setattr(Model, SMARTSQL_ALIAS, s)

# Fixing django.db.models.query.RawQuerySet,
# because indexing and slicing are not performed at the database level
# https://docs.djangoproject.com/en/dev/topics/db/sql/#index-lookups

class PaginatedRawQuerySet(RawQuerySet):
    """Extended RawQuerySet with pagination support"""

    _cache = None

    def count(self):
        """Returns count of rows"""
        sql = self.query.sql
        if self._cache is not None:
            return len(self._cache)
        if not re.compile(r"""^((?:"(?:[^"\\]|\\"|\\\\)*"|'(?:[^'\\]|\\'|\\\\)*'|/\*.*?\*/|--[^\n]*\n|[^"'\\])+)(?:LIMIT|OFFSET).+$""", re.I | re.U | re.S).match(sql):
            sql = re.compile(r"""^((?:"(?:[^"\\]|\\"|\\\\)*"|'(?:[^'\\]|\\'|\\\\)*'|/\*.*?\*/|--[^\n]*\n|[^"'\\])+)ORDER BY[^%]+$""", re.I | re.U | re.S).sub(r'\1', sql)
        sql = "SELECT COUNT(1) as count_value FROM ({0}) as count_list".format(sql)
        cursor = connections[self.query.using].cursor()
        cursor.execute(sql, self.params)
        row = cursor.fetchone()
        return row[0]

    def __len__(self):
        """Returns count of rows"""
        self.fill_cache()
        return len(self._cache)

    def __getitem__(self, k):
        """Returns sliced instance of self.__class__"""
        sql = self.query.sql
        offset, limit = 0, None

        if not isinstance(k, slice):
            return list(self)[k]

        if k.start is not None:
            offset = int(k.start)
        if k.stop is not None:
            end = int(k.stop)
            limit = end - offset
        if limit:
            sql = "{0} LIMIT {1:d}".format(sql, limit)
        if offset:
            sql = "{0} OFFSET {1:d}".format(sql, offset)
        new_cls = self.__class__(
            sql, model=self.model, query=None, params=self.params,
            translations=self.translations, using=self.db
        )
        new_cls.sliced = True
        new_cls.limit = limit
        return new_cls

    def fill_cache(self):
        """Cache for small selections"""
        if self._cache is None:
            self._cache = list(self.iterator())
        return self

    def iterator(self):
        return super(PaginatedRawQuerySet, self).__iter__()

    def __iter__(self):
        """Cache for small selections"""
        self.fill_cache()
        return iter(self._cache)


def raw(self, raw_query, params=None, *args, **kwargs):
    return PaginatedRawQuerySet(raw_query=raw_query, model=self.model,
                                params=params, using=self._db, *args, **kwargs)


def patch_raw_query_set():
    Manager.raw = raw

patch_raw_query_set()
