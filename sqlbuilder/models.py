from django.conf import settings
from django.db.models import Model

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
            setattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS),
                    sqlalchemy.sql.table(cls._meta.db_table))
        return getattr(cls, '_{0}'.format(SQLALCHEMY_ALIAS))

    setattr(Model, SQLALCHEMY_ALIAS, sa)

    # Example of usage:
    # from sqlalchemy.sql import select, table
    # from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    # u = table('user')  # or User.sa
    # p = table('profile')  # or Profile.sa
    # s = select(['*']).select_from(u.join(p, u.vc.id==p.vc.user_id)).where(p.vc.gender == u'M')
    # sc = s.compile(dialect=PGDialect_psycopg2())
    # print unicode(sc), sc.params
    # qs = User.objects.raw(unicode(sc), sc.params)
    # for i in qs: print i
    

except ImportError:
    pass
