===========
SQLBuilder
===========

Contains 2 separated SQLBuilders:

1. SmartSQL, my own lightweight library, with some ideas from `py-smart-sql-constructor <http://code.google.com/p/py-smart-sql-constructor/>`_, but it's not a fork anymore...
2. Extracted slightly `SQLBuilder from SQLObject <http://sqlobject.org/SQLBuilder.html>`_ ORM to be used without the rest of the library (almost non-modified).

Also, SQLBuilder allows to use
`sqlalchemy.sql <http://docs.sqlalchemy.org/en/latest/core/expression_api.html>`_
in Django projects.

LICENSE:

* License is BSD, except third files with license in it's directory, like sqlbuilder/sqlobject/*

Short manual for sqlbuilder.smartsql
=====================================

table:

* "T.base" stand for "base",
* "T.base__a" or "T.base.as_('a')" stand for "base AS a"

field:

* "F.id" stand for "id",
* "F.base__id" or "T.base.id" stand for "base.id"
* "F.base__id__pk" or "F.base__id.as_('pk')" or "T.base.id__pk" or "T.base.id.as_('pk')" stand for "base.id AS pk"

table operator:

* "&" stand for "INNER JOIN"
* "+" stand for "LEFT OUTER JOIN"
* "-" stand for "RIGHT OUTER JOIN"
* "|" stand for "FULL OUTER JOIN"
* "*" stand for "CROSS JOIN"

condition operator:

* "&" stand for "AND"
* "|" stand for "OR"

usage eg:

::

    QS(T.base + T.grade + T.lottery).on(
        (T.base.type == T.grade.item_type) & (T.base.type == 1),
        T.base.type == T.lottery.item_type
    ).fields(
        T.base.type, T.grade.grade, T.lottery.grade
    ).where(
        (T.base.name == "name") & (T.base.status == 0) | (T.base.name == None)
    ).select()

    # step by step

    t = T.grade
    QS(t).select(F.name)

    t = (t * T.base).on(T.grade.item_type == T.base.type)
    QS(t).select(T.grade.name, T.base.img)

    t = (t + T.lottery).on(T.base.type == T.lottery.item_type)
    QS(t).select(T.grade.name, T.base.img, T.lottery.price)

    w = (T.base.type == 1)
    QS(t).where(w).select(T.grade.name, T.base.img, T.lottery.price)

    w = w & (T.grade.status == 0)
    QS(t).where(w).select(T.grade.name, T.base.img, T.lottery.price)

    w = w | (T.lottery.item_type == None)
    QS(t).where(w).select(T.grade.name, T.base.img, T.lottery.price)

    w = w & (T.base.status == 1)
    QS(t).where(w).select(T.grade.name, T.base.img, T.lottery.price)

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

* Grade.ss.t (alias for Grade.ss.table) returns T.grade
* Grade.ss.get_fields() returns [T.grade.id, T.grade.title, ...]
* Grade.ss.qs returns QS(T.grade).fields(Grade.ss.get_fields())

So,

::

    QS(T.grade).where(T.grade.item_type == 'type1')

is equal to:

::

    Grade.ss.qs.where(Grade.ss.t.item_type == 'type1')

How to execute?

::
    
    rows = Grade.objects.raw(*QS(T.grade).where(T.grade.item_type == 'type1').select(Grade.ss.get_fields()))
    # or simple
    rows = Grade.ss.qs.where(Grade.ss.t.item_type == 'type1').select()

Integration sqlbuilder.sqlobject to Django
-------------------------------------------

Example of usage sqlbuilder.sqlobject in Django:

::

    from sqlbuilder.sqlobject import Select, sqlrepr
    from sqlbuilder.models import SQLOBJECT_DIALECT

    # Address is subclass of django.db.models.Model
    s = Select([Address.so.t.name, Address.so.t.state], where=Address.so.name.startswith("sun"))
    # or
    s = Address.so.qs.newItems(Address.so.get_fields()).filter(Address.so.name.startswith("sun"))
    # or simple
    s = Address.so.qs.filter(Address.so.name.startswith("sun"))

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
