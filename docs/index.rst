.. sqlbuilder documentation master file, created by
   sphinx-quickstart on Sat Sep  5 23:02:40 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Lightweight Python SQLBuilder
=============================

Contents:

.. toctree::
   :maxdepth: 10

.. contents:: Table of Contents

SmartSQL - lightweight Python sql builder, follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_, less than 50 Kb.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Ascetic ORM <https://bitbucket.org/emacsway/ascetic>`_, or with super-lightweight datamapper `Openorm <http://code.google.com/p/openorm/source/browse/python/>`_ (`miror <https://bitbucket.org/emacsway/openorm/src/default/python/>`__) etc.

* Home Page: https://bitbucket.org/emacsway/sqlbuilder
* Docs: http://sqlbuilder.readthedocs.org/
* Browse source code: https://bitbucket.org/emacsway/sqlbuilder/src
* Get source code: hg clone https://bitbucket.org/emacsway/sqlbuilder
* Pypi: https://pypi.python.org/pypi/sqlbuilder

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
    ... ).fields(
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
    >>> Q().tables(ta).fields(ta.title).where(ta.title.startswith('A'))
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
    >>> Q().tables(T.book).fields(al).where(al.startswith('A'))
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

    >>> tb.mask << 1
    <LShift: "author"."mask" << %s, [1]>

    >>> tb.mask >> 1
    <RShift: "author"."mask" >> %s, [1]>

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
    <Like: "author"."last_name" LIKE REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['Sm']>

    >>> tb.last_name.istartswith('Sm')
    <Ilike: "author"."last_name" ILIKE REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['Sm']>

    >>> tb.last_name.contains('mi')
    <Like: "author"."last_name" LIKE '%%' || REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['mi']>

    >>> tb.last_name.icontains('mi')
    <Ilike: "author"."last_name" ILIKE '%%' || REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['mi']>

    >>> tb.last_name.endswith('th')
    <Like: "author"."last_name" LIKE '%%' || REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') ESCAPE '!'', ['th']>

    >>> tb.last_name.iendswith('th')
    <Ilike: "author"."last_name" ILIKE '%%' || REPLACE(REPLACE(REPLACE(%s, '!', '!!'), '_', '!_'), '%%', '!%%') ESCAPE '!'', ['th']>

    >>> tb.last_name.rstartswith('Sm')
    <Like: %s LIKE REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['Sm']>

    >>> tb.last_name.ristartswith('Sm')
    <Ilike: %s ILIKE REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['Sm']>

    >>> tb.last_name.rcontains('mi')
    <Like: %s LIKE '%%' || REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['mi']>

    >>> tb.last_name.ricontains('mi')
    <Ilike: %s ILIKE '%%' || REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') || '%%' ESCAPE '!', ['mi']>

    >>> tb.last_name.rendswith('th')
    <Like: %s LIKE '%%' || REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') ESCAPE '!', ['th']>

    >>> tb.last_name.riendswith('th')
    <Ilike: %s ILIKE '%%' || REPLACE(REPLACE(REPLACE("author"."last_name", '!', '!!'), '_', '!_'), '%%', '!%%') ESCAPE '!', ['th']>


    >>> +tb.counter
    <Pos: +"author"."counter", []>

    >>> -tb.counter
    <Neg: -"author"."counter", []>

    >>> ~tb.counter
    <Not: NOT "author"."counter", []>

    >>> tb.name.distinct()
    <Distinct: DISTINCT "author"."name", []>

    >>> tb.counter ** 2
    <Callable: POW("author"."counter", %s), [2]>

    >>> 2 ** tb.counter
    <Callable: POW(%s, "author"."counter"), [2]>

    >>> tb.counter % 2
    <Callable: MOD("author"."counter", %s), [2]>

    >>> 2 % tb.counter
    <Callable: MOD(%s, "author"."counter"), [2]>

    >>> abs(tb.counter)
    <Callable: ABS("author"."counter"), []>

    >>> tb.counter.count()
    <Callable: COUNT("author"."counter"), []>

    >>> tb.age.between(20, 30)
    <Between: "author"."age" BETWEEN %s AND %s, [20, 30]>

    >>> tb.age[20:30]
    <Between: "author"."age" BETWEEN %s AND %s, [20, 30]>

    >>> tb.age[20]
    <Eq: "author"."age" = %s, [20]>

    >>> tb.name.concat(' staff', ' admin')
    <Concat: "author"."name" || %s || %s, [' staff', ' admin']>

    >>> tb.name.concat_ws(' ', 'staff', 'admin')
    <Concat: concat_ws(%s, "author"."name", %s, %s), [' ', 'staff', 'admin']>

    >>> tb.name.op('MY_EXTRA_OPERATOR')(10)
    <Condition: "author"."name" MY_EXTRA_OPERATOR %s, [10]>

    >>> tb.name.rop('MY_EXTRA_OPERATOR')(10)
    <Condition: %s MY_EXTRA_OPERATOR "author"."name", [10]>

    >>> tb.name.asc()
    <Asc: "author"."name" ASC, []>

    >>> tb.name.desc()
    <Desc: "author"."name" DESC, []>

    >>> ((tb.age > 25) | (tb.answers > 10)) & (tb.is_staff | tb.is_admin)
    <And: ("author"."age" > %s OR "author"."answers" > %s) AND ("author"."is_staff" OR "author"."is_admin"), [25, 10]>

    >>> (T.author.first_name != 'Tom') & (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s AND "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>

    >>> (T.author.first_name != 'Tom') | (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s OR "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>


Query object
------------

.. module:: sqlbuilder.smartsql

.. class:: Query

    Query builder class

    .. method:: fields(self, *args, **opts)

        - Adds fields with arguments.
        - Sets fields with single argument of list/tuple type.
        - Gets fields without arguments.
        - Resets fields with ``reset=True`` keyword argument.

        Example of usage::

            >>> from sqlbuilder.smartsql import *
            >>> q = Q().tables(T.author)

            >>> # Add fields:
            >>> q = q.fields(T.author.first_name, T.author.last_name)
            >>> q
            <Query: SELECT "author"."first_name", "author"."last_name" FROM "author", []>
            >>> q = q.fields(T.author.age)
            >>> q
            <Query: SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author", []>

            >>> # Set new fields list:
            >>> q = q.fields([T.author.id, T.author.status])
            >>> q
            <Query: SELECT "author"."id", "author"."status" FROM "author", []>

            >>> # Reset fields:
            >>> q = q.fields([])
            >>> q
            <Query: SELECT  FROM "author", []>

            >>> # Second way to reset fields:
            >>> q = q.fields(reset=True)
            >>> q
            <Query: SELECT  FROM "author", []>

    .. method:: tables(self, tables=None)

        :param tables: Can be None, Table or TableJoin instance
        :type tables: None, Table or TableJoin
        :return: current tables if ``tables`` argument is None, else copied object with new tables
        :rtype: TableJoin if ``tables`` argument is None, else Query

        Example of usage::

            >>> from sqlbuilder.smartsql import Table as T, Query as Q
            >>> q = Q().tables(T.author).fields('*')
            >>> q
            <Query: SELECT * FROM "author", []>
            >>> q = q.tables(T.author.as_('author_alias'))
            >>> q
            <Query: SELECT * FROM "author" AS "author_alias", []>
            >>> q.tables()
            <TableJoin: "author" AS "author_alias", []>
            >>> q = q.tables((q.tables() + T.book).on(T.book.author_id == T.author.as_('author_alias').id))
            >>> q
            <Query: SELECT * FROM "author" AS "author_alias" LEFT OUTER JOIN "book" ON ("book"."author_id" = "author_alias"."id"), []>

    .. method:: where(self, cond, op=operator.and_)

        - Adds new criterias using the ``op`` operator, if ``op`` is not None.
        - Sets new criterias if ``op`` is None.

        :param cond: Selection criterias
        :type cond: Expr or subclass
        :param op: Attribute of ``operator`` module or None, ``operator.and_`` by default
        :return: copy of Query instance with new criteria
        :rtype: Query

        Example of usage::

            >>> import operator
            >>> from sqlbuilder.smartsql import Table as T, Query as Q
            >>> q = Q().tables(T.author).fields('*')
            >>> q
            <Query: SELECT * FROM "author", []>
            >>> q = q.where(T.author.is_staff.is_(True))
            >>> q
            <Query: SELECT * FROM "author" WHERE "author"."is_staff" IS %s, [True]>
            >>> q = q.where(T.author.first_name == 'John')
            >>> q
            <Query: SELECT * FROM "author" WHERE "author"."is_staff" IS %s AND "author"."first_name" = %s, [True, 'John']>
            >>> q = q.where(T.author.last_name == 'Smith', op=operator.or_)
            >>> q
            <Query: SELECT * FROM "author" WHERE "author"."is_staff" IS %s AND "author"."first_name" = %s OR "author"."last_name" = %s, [True, 'John', 'Smith']>
            >>> q = q.where(T.author.last_name == 'Smith', op=None)
            >>> q
            <Query: SELECT * FROM "author" WHERE "author"."last_name" = %s, ['Smith']>

    .. method:: order_by(self, *args, **opts)

        This method has interface similar to :meth:`~fields`

        - Adds expressions if arguments exists.
        - Sets expressions if exists single argument of list/tuple type.
        - Gets expressions without arguments.
        - Resets expressions with ``reset=True`` keyword argument.

        Example of usage::

            >>> from sqlbuilder.smartsql import Table as T, Query as Q
            >>> q = Q().tables(T.author).fields('*')
            >>> q
            <Query: SELECT * FROM "author", []>

            >>> # Adds expressions
            >>> q = q.order_by(T.author.first_name, T.author.last_name)
            >>> q
            <Query: SELECT * FROM "author" ORDER BY "author"."first_name" ASC, "author"."last_name" ASC, []>
            >>> q = q.order_by(T.author.age.desc())
            >>> q

            # Set new expressions list:
            <Query: SELECT * FROM "author" ORDER BY "author"."first_name" ASC, "author"."last_name" ASC, "author"."age" DESC, []>
            >>> q = q.order_by([T.author.id.desc(), T.author.status])
            >>> q
            <Query: SELECT * FROM "author" ORDER BY "author"."id" DESC, "author"."status" ASC, []>

            # Reset expressions
            >>> q = q.order_by([])
            >>> q
            <Query: SELECT * FROM "author", []>

            >>> # Second way to reset expressions:
            >>> q = q.order_by(reset=True)
            >>> q
            <Query: SELECT * FROM "author", []>


Compilers
---------

There are three compilers for three dialects:

- ``sqlbuilder.smartsql.compile(expr, state=None)`` - is a default compiler with PostgreSQL dialect.
- ``sqlbuilder.smartsql.compilers.mysql.compile(expr, state=None)`` - has MySQL dialect.
- ``sqlbuilder.smartsql.compilers.sqlite.compile(expr, state=None)`` - has SQLite dialect.



Django integration
==================

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

