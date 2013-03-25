===========
SQLBuilder
===========

SmartSQL - lightweight sql builder.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Autumn ORM <https://bitbucket.org/evotech/autumn>`_.

SQLBuilder integration to Django also allows to use external sqlbuilders, like `SQLBuilder from SQLObject <http://sqlobject.org/SQLBuilder.html>`_ or `sqlalchemy.sql <http://docs.sqlalchemy.org/en/latest/core/expression_api.html>`_.


LICENSE:

* License is BSD

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

Simple add "sqlbuilder.django_sqlbuilder" to your INSTALLED_APPS.

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

    Grade.ss.qs.where(Grade.ss.item_type == 'type1')

How to execute?

::
    
    rows = Grade.ss.qs.where(Grade.ss.item_type == 'type1').select()
    # Also is possible
    rows = Grade.objects.raw(*QS(T.grade).where(T.grade.item_type == 'type1').select(Grade.ss.get_fields()))

Paginator
==========
django.db.models.query.RawQuerySet `indexing and slicing are not performed at the database level
<https://docs.djangoproject.com/en/dev/topics/db/sql/#index-lookups>`_,
so it can cause problems with pagination.

For this reason, SQLBuilder fixes this issue.




Integration of third-party sqlbuilders.
========================================

Integration sqlbuilder.sqlobject to Django
-------------------------------------------

Integration sqlobject to Django:

::

    from sqlobject.sqlbuilder import Select, sqlrepr
    from sqlbuilder.models import SQLOBJECT_DIALECT

    # Address is subclass of django.db.models.Model
    t = Address.so.t
    s = Select([t.name, t.state], where=t.name.startswith("sun"))
    # or
    s = Address.so.qs.newItems(Address.so.get_fields()).filter(t.name.startswith("sun"))
    # or simple
    s = Address.so.qs.filter(t.name.startswith("sun"))

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
