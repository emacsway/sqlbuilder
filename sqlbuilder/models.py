from __future__ import absolute_import, unicode_literals
import re
from django.conf import settings
from django.db import connection, connections
from django.db.models import Model
from django.db.models.manager import Manager
from django.db.models.query import RawQuerySet
from django.utils.importlib import import_module

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
        """Returns table instance."""
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
        """Sets query set."""
        return self.get_query_set()

    # Aliases
    @property
    def t(self):
        """Returns table instance."""
        return self._table


if SMARTSQL_USE:
    from . import smartsql

    class DjQS(smartsql.QS):
        """Query Set adapted for Django."""

        def __len__(self):
            """Returns length or list."""
            return len(self.execute())

        def count(self):
            """Returns length or list."""
            return len(self.execute())

        def __iter__(self):
            """Returns iterator."""
            return iter(self.execute())

        def __getitem__(self, key):
            """Returns sliced self or item."""
            return self.execute()[key]

        def execute(self):
            """Implementation of query execution"""
            return self.django.model.objects.raw(
                smartsql.sqlrepr(self), smartsql.sqlparams(self)
            )

        def result(self):
            """Result"""
            if self._action in ('select', 'count', ):
                return self
            return self.execute()

    class SmartSQLFacade(AbstractFacade):
        """Abstract facade for Django integration"""

        def __init__(self, model):
            """Constructor"""
            self._model = model

            if hasattr(self._model.objects, 'localize_fieldname'):

                class MlTable(smartsql.Table):
                    def __getattr__(self, name):
                        if name[0] == '_':
                            raise AttributeError
                        parts = name.split('__')
                        parts[0] = self.django.model.objects.localize_fieldname(parts[0])
                        return super(MlTable, self).__getattr__('__'.join(parts))

                self._table = MlTable(self._model._meta.db_table)
            else:
                self._table = smartsql.Table(self._model._meta.db_table)

            self._table.django = self
            self._query_set = DjQS(self.table).fields(self.get_fields())
            self._query_set.django = self

        def get_fields(self, prefix=None):
            """Returns field list."""
            if prefix is None:
                prefix = self._table
            result = []
            for f in self._model._meta.fields:
                if f.column:
                    result.append(smartsql.Field(f.column, prefix))
            return result

    @classproperty
    def ss(cls):
        if getattr(cls, '_{0}'.format(SMARTSQL_ALIAS), None) is None:
            setattr(cls, '_{0}'.format(SMARTSQL_ALIAS), SmartSQLFacade(cls))
        return getattr(cls, '_{0}'.format(SMARTSQL_ALIAS))

    setattr(Model, SMARTSQL_ALIAS, ss)

if SQLOBJECT_USE:
    from . import sqlobject

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
            self._table = sqlobject.Table(self._model._meta.db_table)
            self._query_set = sqlobject.Select(
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

    _result_cache = None

    def count(self):
        """Returns count of rows"""
        sql = self.query.sql
        self._make_cache_conditional()
        if self._result_cache is not None:
            return len(self._result_cache)
        if not re.compile(r"""^((?:"(?:[^"\\]|\\"|\\\\)*"|'(?:[^'\\]|\\'|\\\\)*'|/\*.*?\*/|--[^\n]*\n|[^"'\\])+)(?:LIMIT|OFFSET).+$""", re.I|re.U|re.S).match(sql):
            sql = re.compile(r"""^((?:"(?:[^"\\]|\\"|\\\\)*"|'(?:[^'\\]|\\'|\\\\)*'|/\*.*?\*/|--[^\n]*\n|[^"'\\])+)ORDER BY[^%]+$""", re.I|re.U|re.S).sub(r'\1', sql)
        sql = "SELECT COUNT(1) as c FROM ({0}) as t".format(sql)
        cursor = connections[self.query.using].cursor()
        cursor.execute(sql, self.params)
        row = cursor.fetchone()
        return row[0]

    def __len__(self):
        """Returns count of rows"""
        return self.count()

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

    def _make_cache_conditional(self):
        """Cache for small selections"""
        if getattr(self, 'sliced', False) and getattr(self, 'limit', 0) < 300:
            if self._result_cache is None:
                self._result_cache = [v for v in super(PaginatedRawQuerySet, self).__iter__()]

    def __iter__(self):
        """Cache for small selections"""
        self._make_cache_conditional()
        if self._result_cache is not None:
            for v in self._result_cache:
                yield v
        else:
            for v in super(PaginatedRawQuerySet, self).__iter__():
                yield v


def raw(self, raw_query, params=None, *args, **kwargs):
    return PaginatedRawQuerySet(raw_query=raw_query, model=self.model, params=params, using=self._db, *args, **kwargs)


def patch_raw_query_set():
    Manager.raw = raw

patch_raw_query_set()
