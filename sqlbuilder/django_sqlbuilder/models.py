from __future__ import absolute_import, unicode_literals
import re
from django.conf import settings
from django.db import connection, connections
from django.db.models import Model
from django.db.models.manager import Manager
from django.db.models.query import RawQuerySet
from django.utils.importlib import import_module
from .signals import field_conversion

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 'ss')
SMARTSQL_USE = getattr(settings, 'SQLBUILDER_SMARTSQL_USE', True)

SQLOBJECT_ALIAS = getattr(settings, 'SQLBUILDER_SQLOBJECT_ALIAS', 'so')
SQLOBJECT_USE = getattr(settings, 'SQLBUILDER_SQLOBJECT_USE', True)

SQLALCHEMY_ALIAS = getattr(settings, 'SQLBUILDER_SQLALCHEMY_ALIAS', 'sa')
SQLALCHEMY_USE = getattr(settings, 'SQLBUILDER_SQLALCHEMY_USE', True)


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class AbstractFacade(object):
    """Abstract facade for Django integration"""
    _model = None
    _table = None
    _query_set = None

    def __init__(self, model):
        """Constructor"""
        raise NotImplementedError

    @property
    def model(self):
        """Returns Django model instance."""
        return self._model

    @property
    def table(self):
        """Returns table instance."""
        return self._table

    def get_fields(self, prefix=None):
        """Returns fileld list."""
        raise NotImplementedError

    def set_query_set(self, query_set):
        """Sets query set."""
        self._query_set = query_set
        return self

    def get_query_set(self):
        """Returns query set."""
        return self._query_set

    @property
    def qs(self):
        """Returns query set."""
        return self.get_query_set()

    # Aliases
    @property
    def t(self):
        """Returns table instance."""
        return self._table


if SMARTSQL_USE:
    from .. import smartsql

    SMARTSQL_DIALECTS = {
        'sqlite3': 'sqlite',
        'mysql': 'mysql',
        'postgresql': 'postgres',
        'postgresql_psycopg2': 'postgres',
        'postgis': 'postgres',
        'oracle': 'oracle',
    }

    class QS(smartsql.QS):
        """Query Set adapted for Django."""

        _cache = None

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
            self.using = qs.using
            self.base_table = qs.base_table

    class Table(smartsql.Table):
        """Table class for Django model"""

        def __init__(self, model, *args, **kwargs):
            """Constructor"""
            super(Table, self).__init__(model._meta.db_table, *args, **kwargs)
            self.model = model
            self.qs = kwargs.pop('qs', QS(self).fields(self.get_fields()))
            self.qs.base_table = self
            self.qs.model = self.model

        def get_fields(self, prefix=None):
            """Returns field list."""
            if prefix is None:
                prefix = self
            result = []
            for f in self.model._meta.fields:
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
            return self.table.model

    @classproperty
    def ss(cls):
        if getattr(cls, '_{0}'.format(SMARTSQL_ALIAS), None) is None:
            setattr(cls, '_{0}'.format(SMARTSQL_ALIAS), Table(cls))
        return getattr(cls, '_{0}'.format(SMARTSQL_ALIAS))

    setattr(Model, SMARTSQL_ALIAS, ss)

try:
    if not SQLOBJECT_USE:
        raise ImportError
    import sqlobject.sqlbuilder

    SQLOBJECT_DIALECTS = {
        'sqlite3': 'sqlite',
        'mysql': 'mysql',
        'postgresql': 'postgres',
        'postgresql_psycopg2': 'postgres',
        'postgis': 'postgres',
        'oracle': 'oracle',
    }

    def get_so_dialect():
        """Returns instance of Dialect"""
        engine = connection.settings_dict['ENGINE'].rsplit('.')[-1]
        return SQLOBJECT_DIALECTS[engine]

    SQLOBJECT_DIALECT = get_so_dialect()
    settings.SQLBUILDER_SQLOBJECT_DIALECT = SQLOBJECT_DIALECT

    class SQLObjectFacade(AbstractFacade):
        """Abstract facade for Django integration"""

        def __init__(self, model):
            """Constructor"""
            self._model = model
            self._table = sqlobject.sqlbuilder.Table(self._model._meta.db_table)
            self._query_set = sqlobject.sqlbuilder.Select(
                items=self.get_fields(),
                staticTables=[self.table, ]
            )

        def get_fields(self, prefix=None):
            """Returns field list."""
            if prefix is None:
                prefix = self._table
            result = []
            for f in self._model._meta.fields:
                if f.column:
                    result.append(getattr(prefix, f.column))
            return result

    @classproperty
    def so(cls):
        if getattr(cls, '_{0}'.format(SQLOBJECT_ALIAS), None) is None:
            setattr(cls, '_{0}'.format(SQLOBJECT_ALIAS), SQLObjectFacade(cls))
        return getattr(cls, '_{0}'.format(SQLOBJECT_ALIAS))

    setattr(Model, SQLOBJECT_ALIAS, so)

except ImportError:
    pass

try:
    if not SQLALCHEMY_USE:
        raise ImportError
    import sqlalchemy.sql

    SQLALCHEMY_DIALECTS = {
        'sqlite3': 'sqlalchemy.dialects.sqlite.pysqlite.SQLiteDialect_pysqlite',
        'mysql': 'sqlalchemy.dialects.mysql.mysqldb.MySQLDialect_mysqldb',
        'postgresql': 'sqlalchemy.dialects.postgresql.pypostgresql.PGDialect_pypostgresql',
        'postgresql_psycopg2': 'sqlalchemy.dialects.postgresql.psycopg2.PGDialect_psycopg2',
        'postgis': 'sqlalchemy.dialects.postgresql.psycopg2.PGDialect_psycopg2',
        'oracle': 'sqlalchemy.dialects.oracle.cx_oracle.OracleDialect_cx_oracle',
    }

    def get_sa_dialect():
        """Returns instance of Dialect"""
        engine = connection.settings_dict['ENGINE'].rsplit('.')[-1]
        module_name, cls_name = SQLALCHEMY_DIALECTS[engine].rsplit('.', 1)
        module = import_module(module_name)
        cls = getattr(module, cls_name)
        return cls()

    SQLALCHEMY_DIALECT = get_sa_dialect()
    settings.SQLBUILDER_SQLALCHEMY_DIALECT = SQLALCHEMY_DIALECT

    class VirtualColumns(object):
        """Virtual column class."""
        _table = None
        _columns = None

        def __init__(self, table=None):
            """Constructor"""
            self._table = table
            self._columns = {}

        def __getattr__(self, name):
            """Creates column on fly."""
            if name not in self._columns:
                c = sqlalchemy.sql.column(name)
                c.table = self._table
                self._columns[name] = c
            return self._columns[name]

    @property
    def vc(self):
        """Returns VirtualColumns instance"""
        if getattr(self, '_vc', None) is None:
            self._vc = VirtualColumns(self)
        return self._vc

    sqlalchemy.sql.TableClause.vc = vc

    class SQLAlchemyFacade(AbstractFacade):
        """Abstract facade for Django integration"""

        dialect = SQLALCHEMY_DIALECT

        def __init__(self, model):
            """Constructor"""
            self._model = model
            self._table = sqlalchemy.sql.table(self._model._meta.db_table)
            self._query_set = sqlalchemy.sql.select(self.get_fields())\
                .select_from(self.table)

        def get_fields(self, prefix=None):
            """Returns field list."""
            if prefix is None:
                prefix = self._table
            result = []
            for f in self._model._meta.fields:
                if f.column:
                    result.append(getattr(self._table.vc, f.column))
            return result

    @classproperty
    def sa(cls):
        if getattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS), None) is None:
            setattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS), SQLAlchemyFacade(cls))
        return getattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS))

    setattr(Model, SQLALCHEMY_ALIAS, sa)

except ImportError:
    pass

# Fixing django.db.models.query.RawQuerySet


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
    return PaginatedRawQuerySet(raw_query=raw_query, model=self.model, params=params, using=self._db, *args, **kwargs)


def patch_raw_query_set():
    Manager.raw = raw

patch_raw_query_set()
