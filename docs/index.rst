.. sqlbuilder documentation master file, created by
   sphinx-quickstart on Sat Sep  5 23:02:40 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Lightweight Python SQLBuilder
=============================

Contents:

.. toctree::
   :maxdepth: 2

.. contents:: Table of Contents

SmartSQL - lightweight Python sql builder, follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_, less than 50 Kb.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Ascetic ORM <https://bitbucket.org/emacsway/ascetic>`_, or with super-lightweight datamapper `Openorm <http://code.google.com/p/openorm/source/browse/python/>`_ (`miror <https://bitbucket.org/emacsway/openorm/src/default/python/>`__) etc.

| Home Page: https://bitbucket.org/emacsway/sqlbuilder
| Docs: http://sqlbuilder.readthedocs.org/

LICENSE:

* License is BSD

Short manual for sqlbuilder.smartsql
=====================================


Quick start
-----------

::

    >>> from sqlbuilder.smartsql import Q, T, compile
    >>> compile(Q().tables(
    ...     (T.book & T.author).on(T.book.author_id == T.author.id)
    ... ).columns(
    ...     T.book.name, T.author.first_name, T.author.last_name
    ... ).where(
    ...     (T.author.first_name != 'Tom') & (T.author.last_name != 'Smith')
    ... )[20:30])
    ('SELECT "book"."name", "author"."first_name", "author"."last_name" FROM "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id") WHERE "author"."first_name" <> %s AND "author"."last_name" <> %s LIMIT %s OFFSET %s', ['Tom', 'Smith', 10, 20])


Table
-----

::

    >>> from sqlbuilder.smartsql import Table as T, Query as Q

    >>> T.book
    <Table: "book", []>

    >>> T.book__a
    <TableAlias: "a", []>

    >>> T.book.as_('a')  # Same as T.book__a
    <TableAlias: "a", []>

Compiling instance of TableAlias depends on context of usage::

    >>> ta = T.book.as_('a')
    >>> ta
    <TableAlias: "a", []>
    >>> Q().tables(ta).columns(ta.title).where(ta.title.startswith('A'))
    <Query: SELECT "a"."title" FROM "book" AS "a" WHERE "a"."title" LIKE %s || %s, ['A', '%']>



Field
-----

::

    >>> from sqlbuilder.smartsql import Table as T, Field as F, Query as Q

    >>> T.book.name
    <Field: "book"."name", []>

    >>> T.book.name.as_('a')
    <Alias: "a", []>

    >>> F.book__name  # Same as T.book.name
    <Field: "book"."name", []>

    >>> F.book__name__a  # T.book.name.as_('a')
    <Alias: "a", []>

    >>> F.book__name.as_('a')  # T.book.name.as_('a') or F.book__name__a
    <Alias: "a", []>

Compiling instance of TableAlias depends on context of usage::

    >>> al = T.book.name.as_('a')
    >>> al
    <Alias: "a", []>
    >>> Q().tables(T.book).columns(al).where(al.startswith('A'))
    <Query: SELECT "book"."name" AS "a" FROM "book" WHERE "a" LIKE %s || %s, ['A', '%']>


Table operators
---------------

::

    >>> (T.book & T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book + T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" LEFT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book - T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" RIGHT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book | T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" FULL OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book * T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" CROSS JOIN "author" ON ("book"."author_id" = "author"."id"), []>


Condition operators
-------------------

::

    >>> from sqlbuilder.smartsql import Table as T, Param as P
    >>> tb = T.author

    >>> tb.name == 'Tom'
    <Eq: "author"."name" = %s, ['Tom']>

    >>> tb.name != 'Tom'
    <Ne: "author"."name" <> %s, ['Tom']>

    >>> tb.counter + 1
    <Add: "author"."counter" + %s, [1]>

    >>> 1 + tb.counter
    <Add: %s + "author"."counter", [1]>

    >>> tb.counter - 1
    <Sub: "author"."counter" - %s, [1]>

    >>> 10 - tb.counter
    <Sub: %s - "author"."counter", [10]>

    >>> tb.counter * 2
    <Mul: "author"."counter" * %s, [2]>

    >>> 2 * tb.counter
    <Mul: %s * "author"."counter", [2]>

    >>> tb.counter / 2
    <Div: "author"."counter" / %s, [2]>

    >>> 10 / tb.counter
    <Div: %s / "author"."counter", [10]>

    >>> tb.is_staff & tb.is_admin
    <And: "author"."is_staff" AND "author"."is_admin", []>

    >>> tb.is_staff | tb.is_admin
    <Or: "author"."is_staff" OR "author"."is_admin", []>

    >>> tb.counter > 10
    <Gt: "author"."counter" > %s, [10]>

    >>> 10 > tb.counter
    <Lt: "author"."counter" < %s, [10]>

    >>> tb.counter >= 10
    <Ge: "author"."counter" >= %s, [10]>

    >>> 10 >= tb.counter
    <Le: "author"."counter" <= %s, [10]>

    >>> tb.counter < 10
    <Lt: "author"."counter" < %s, [10]>

    >>> 10 < tb.counter
    <Gt: "author"."counter" > %s, [10]>

    >>> tb.counter <= 10
    <Le: "author"."counter" <= %s, [10]>

    >>> 10 <= tb.counter
    <Ge: "author"."counter" >= %s, [10]>

    >>> tb.is_staff.is_(True)
    <Is: "author"."is_staff" IS %s, [True]>

    >>> tb.is_staff.is_not(True)
    <IsNot: "author"."is_staff" IS NOT %s, [True]>

    >>> tb.status.in_(('new', 'approved'))
    <In: "author"."status" IN (%s, %s), ['new', 'approved']>

    >>> tb.status.not_in(('new', 'approved'))
    <NotIn: "author"."status" NOT IN (%s, %s), ['new', 'approved']>

    >>> tb.status.in_(('new', 'approved'))
    <In: "author"."status" IN (%s, %s), ['new', 'approved']>

    >>> tb.status.not_in(('new', 'approved'))
    <NotIn: "author"."status" NOT IN (%s, %s), ['new', 'approved']>


    >>> tb.last_name.like('mi')
    <Like: "author"."last_name" LIKE %s, ['mi']>

    >>> tb.last_name.ilike('mi')
    <Ilike: "author"."last_name" ILIKE %s, ['mi']>

    >>> P('mi').like(tb.last_name)
    <Like: %s LIKE "author"."last_name", ['mi']>

    >>> tb.last_name.rlike('mi')
    <Like: %s LIKE "author"."last_name", ['mi']>

    >>> tb.last_name.rilike('mi')
    <Ilike: %s ILIKE "author"."last_name", ['mi']>

    >>> tb.last_name.startswith('Sm')
    <Like: "author"."last_name" LIKE %s || %s, ['Sm', '%']>

    >>> tb.last_name.istartswith('Sm')
    <Ilike: "author"."last_name" ILIKE %s || %s, ['Sm', '%']>

    >>> tb.last_name.contains('mi')
    <Like: "author"."last_name" LIKE %s || %s || %s, ['%', 'mi', '%']>

    >>> tb.last_name.icontains('mi')
    <Ilike: "author"."last_name" ILIKE %s || %s || %s, ['%', 'mi', '%']>

    >>> tb.last_name.endswith('th')
    <Like: "author"."last_name" LIKE %s || %s, ['%', 'th']>

    >>> tb.last_name.iendswith('th')
    <Ilike: "author"."last_name" ILIKE %s || %s, ['%', 'th']>

    >>> tb.last_name.rstartswith('Sm')
    <Like: %s || %s LIKE "author"."last_name", ['Sm', '%']>

    >>> tb.last_name.ristartswith('Sm')
    <Ilike: %s || %s ILIKE "author"."last_name", ['Sm', '%']>

    >>> tb.last_name.rcontains('mi')
    <Like: %s || %s || %s LIKE "author"."last_name", ['%', 'mi', '%']>

    >>> tb.last_name.ricontains('mi')
    <Ilike: %s || %s || %s ILIKE "author"."last_name", ['%', 'mi', '%']>

    >>> tb.last_name.rendswith('th')
    <Like: %s || %s LIKE "author"."last_name", ['%', 'th']>

    >>> tb.last_name.riendswith('th')
    <Ilike: %s || %s ILIKE "author"."last_name", ['%', 'th']>


    >>> (T.author.first_name != 'Tom') & (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s AND "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>

    >>> (T.author.first_name != 'Tom') | (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s OR "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>

    >>> T.author.age.between(20, 30)
    <Between: "author"."age" BETWEEN %s AND %s, [20, 30]>


Compilers
---------

There are three compilers for three dialects:

- ``sqlbuilder.smartsql.compile(expr, state=None)`` - is a default compiler with PostgreSQL dialect.
- ``sqlbuilder.smartsql.compilers.mysql.compile(expr, state=None)`` - has MySQL dialect.
- ``sqlbuilder.smartsql.compilers.sqlite.compile(expr, state=None)`` - has SQLite dialect.



Django integration.
=====================

Simple add "sqlbuilder.django_sqlbuilder" to your INSTALLED_APPS.

::

    >>> object_list = Book.s.q.tables(
    ...     (Book.s & Author.s).on(Book.s.author == Author.s.pk)
    ... ).where(
    ...     (Author.s.first_name != 'James') & (Author.s.last_name != 'Joyce')
    ... )[:10]


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

