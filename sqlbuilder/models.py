from django.conf import settings
from django.db import connection, connections
from django.db.models import Model
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

if SMARTSQL_USE:
    import smartsql

    @classproperty
    def ss(cls):
        return getattr(
            smartsql.Table,
            cls._meta.db_table
        )

    setattr(Model, SMARTSQL_ALIAS, ss)

if SQLOBJECT_USE:
    import sqlobject

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

    @classproperty
    def so(cls):
        return getattr(
            sqlobject.table,
            cls._meta.db_table
        )

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

    @classproperty
    def sa(cls):
        if getattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS), None) is None:
            table = sqlalchemy.sql.table(cls._meta.db_table)
            table.dialect = SQLALCHEMY_DIALECT
            setattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS), table)
        return getattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS))

    setattr(Model, SQLALCHEMY_ALIAS, sa)

except ImportError:
    pass

# Fixing django.db.models.query.RawQuerySet


def count(self):
    """Returns count of rows"""
    sql = u"SELECT COUNT(1) as c FROM ({0}) as t".format(self.query.sql)
    cursor = connections[self.query.using].cursor()
    cursor.execute(sql, self.params)
    row = cursor.fetchone()
    return row[0]


def __getslice__(self, i, j):
    """Returns sliced instance of self.__class__"""
    i = i or 0
    sql = self.query.sql
    if j:
        limit = j - i
        if limit > 0:
            sql = u"{0} LIMIT {1:d}".format(sql, limit)
    if i:
        sql = u"{0} OFFSET {1:d}".format(sql, i)
    return self.__class__(sql, model=self.model, query=None,
                          params=self.params, translations=self.translations,
                          using=self.db)

if not hasattr(RawQuerySet, 'count'):
    RawQuerySet.count = count
if not hasattr(RawQuerySet, '__len__'):
    RawQuerySet.__len__ = count
if not hasattr(RawQuerySet, '__getslice__'):
    RawQuerySet.__getslice__ = __getslice__
