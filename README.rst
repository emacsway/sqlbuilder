===========
SQLBuilder
===========

Contains 2 separated SQLBuilders:

1. Fork from `py-smart-sql-constructor <http://code.google.com/p/py-smart-sql-constructor/>`_ (modified).
2. Extracted slightly `SQLBuilder from SQLObject <http://sqlobject.org/SQLBuilder.html>`_ ORM to be used without the rest of the library (almost non-modified).

Also, SQLBuilder allows to use
`sqlalchemy.sql <http://docs.sqlalchemy.org/en/latest/core/expression_api.html>`_
in Django projects.

LICENSE:

1. sqlbuilder/sqlobject/* license is LGPL (extracted from SQLObject).
2. sqlbuilder/smartsql/* and rest files - license is BSD.

See also:

  * http://ivan.allindustry.net/blog/2011/02/13/django-plyusy-i-minusy/ (Russian)
  * http://ivan.allindustry.net/blog/2012/02/11/django-improved-sqlbuilder/ (Russian)
  * http://ivan.allindustry.net/en/blog/2012/02/11/django-improved-sqlbuilder/ (English)

Short manual for sqlbuilder.smartsql
=====================================

table: "T.base" stand for "base", "T.base__a" stand for "base AS a"

field: "F.id" stand for "id", "F.base__id" stand for "base.id"

table operator: "*" stand for "JOIN", "+" stand for "LEFT JOIN"

condition operator: "&" stand for "AND", "|" stand for "OR"

usage eg:

::

    QS(T.base + T.grade + T.lottery).on(
        (F.base__type == F.grade__item_type) & (F.base__type == 1),
        F.base__type == F.lottery__item_type
    ).where(
        (F.name == "name") & (F.status == 0) | (F.name == None)
    ).select(F.type, F.grade__grade, F.lottery__grade)

    # step by step

    t = T.grade
    QS(t).select(F.name)

    t = (t * T.base).on(F.grade__item_type == F.base__type)
    QS(t).select(F.grade__name, F.base__img)

    t = (t + T.lottery).on(F.base__type == F.lottery__item_type)
    QS(t).select(F.grade__name, F.base__img, F.lottery__price)

    w = (F.base__type == 1)
    QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price)

    w = w & (F.grade__status == 0)
    QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price)

    w = w | (F.lottery__item_type == None)
    QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price)

    w = w & (F.base__status == 1)
    QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price)

My improvement:
----------------

T.grade.item_type is equal to F.grade__item_type

So,

::

    t = T.grade
    t = (t * T.base).on(F.grade__item_type == F.base__type)

is equal to:

::

    t1 = T.grade
    t2 = T.base
    t = (t1 * t2).on(t1.item_type == t2.type)

Django integration.
=====================

Simple add "sqlbuilder" to your INSTALLED_APPS.


Integration sqlbuilder.smartsql to Django
------------------------------------------

For Django model

::

    class Grade(django.db.models.Model):
        # ...
        class Meta:
            db_table = "grade"

* T.grade is equal Grade.ss.table or simple Grade.ss.t
* QS(T.grade) is equal Grade.ss.qs
* [T.grade.id, T.grade.title, ...] is equal Grade.ss.get_fields()


So,

::

    QS(T.grade).where(F.grade__item_type == 'type1')

is equal to:

::

    Grade.ss.qs.where(Grade.ss.t.item_type == 'type1')

How to execute?

::
    
    rows = Grade.objects.raw(*Grade.ss.qs.where(Grade.ss.t.item_type == 'type1').select("*"))
    # or
    rows = Grade.objects.raw(*Grade.ss.qs.where(Grade.ss.t.item_type == 'type1').select(Grade.ss.get_fields()))
    # or simple
    rows = Grade.objects.raw(*Grade.ss.qs.where(Grade.ss.t.item_type == 'type1').select())

Integration sqlbuilder.sqlobject to Django
-------------------------------------------

Example of usage sqlbuilder.sqlobject in Django:

::

    from sqlbuilder.sqlobject import Select, LIKE, sqlrepr
    from sqlbuilder.models import SQLOBJECT_DIALECT

    # Address is subclass of django.db.models.Model
    s = Select([Address.so.t.name, Address.so.t.state], where=LIKE(Address.so.name, "%ian%"))
    # or
    s = Address.so.qs.newItems(Address.so.get_fields())
    # or simple
    s = Address.so.qs

    rows = Address.objects.raw(sqlrepr(s, SQLOBJECT_DIALECT))

Integration sqlalchemy.sql to Django
-------------------------------------

SQLBuilder library does not contains
`sqlalchemy.sql`_,
so, you need to install additionally sqlalchemy to your Python environment.

Example of usage sqlalchemy.sql in Django:

::

    from sqlalchemy.sql import select, table
    from sqlbuilder.models import SQLALCHEMY_DIALECT
    
    # User, Profile is subclasses of django.db.models.Model
    dialect = User.sa.dialect  # or SQLALCHEMY_DIALECT
    u = User.sa.t  # or table('user')
    p = Profile.sa.t  # or table('profile')
    s = select(['*']).select_from(u.join(p, u.vc.id==p.vc.user_id)).where(p.vc.gender == u'M')
    sc = s.compile(dialect=dialect)
    rows = User.objects.raw(unicode(sc), sc.params)
    for row in rows:
        print row

Paginator
==========
django.db.models.query.RawQuerySet `does not supports __len__() and __getitem__()
<https://docs.djangoproject.com/en/dev/topics/db/sql/#index-lookups>`_ methods,
so it can cause problems with pagination.

For this reason, SQLBuilder fixes this issue.
