===========
SQLBuilder
===========

SmartSQL - lightweight Python sql builder, follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_, less than 50 Kb.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Ascetic ORM <https://bitbucket.org/emacsway/ascetic>`_, or with super-lightweight datamapper `Openorm <http://code.google.com/p/openorm/source/browse/python/>`_ (`miror <https://bitbucket.org/emacsway/openorm/src/default/python/>`__) etc.

Home Page: https://bitbucket.org/emacsway/sqlbuilder

LICENSE:

* License is BSD

Short manual for sqlbuilder.smartsql
=====================================

Quick start::

    >>> from sqlbuilder.smartsql import Q, T, compile
    >>> compile(Q().tables(
    ...     (T.book & T.author).on(T.book.author_id == T.author.id)
    ... ).columns(
    ...     T.book.name, T.author.first_name, T.author.last_name
    ... ).where(
    ...     (T.author.first_name != 'Tom') & (T.author.last_name != 'Smith')
    ... )[20:30])
    ('SELECT "book"."name", "author"."first_name", "author"."last_name" FROM "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id") WHERE "author"."first_name" <> %s AND "author"."last_name" <> %s LIMIT %s OFFSET %s', ['Tom', 'Smith', 10, 20])

table::

    >>> T.book
    <Table: "book", []>

    >>> T.book__a
    <TableAlias: "a", []>

    >>> T.book.as_('a')
    <TableAlias: "a", []>

field::

    >>> T.book.name
    <Field: "book"."name", []>

    >>> T.book.name.as_('a')
    <Alias: "a", []>

    >>> F.book__name
    <Field: "book"."name", []>

    >>> F.book__name__a
    <Alias: "a", []>

    >>> F.book__name.as_('a')
    <Alias: "a", []>


table operator::

    >>> (T.book & T.author).on(T.book.author_id == T.author.id)
    Out[4]: <TableJoin: "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book + T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" LEFT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book - T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" RIGHT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book | T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" FULL OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    >>> (T.book * T.author).on(T.book.author_id == T.author.id)
    <TableJoin: "book" CROSS JOIN "author" ON ("book"."author_id" = "author"."id"), []>

condition operator::

    >>> (T.author.first_name != 'Tom') & (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s AND "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>

    >>> (T.author.first_name != 'Tom') | (T.author.last_name.in_(('Smith', 'Johnson')))
    <Condition: "author"."first_name" <> %s OR "author"."last_name" IN (%s, %s), ['Tom', 'Smith', 'Johnson']>

    >>> T.author.last_name.startswith('Sm')
    <Condition: "author"."last_name" LIKE %s || %s, ['Sm', u'%']>

    >>> T.author.last_name.istartswith('Sm')
    <Condition: "author"."last_name" ILIKE %s || %s, ['Sm', u'%']>

    >>> T.author.last_name.contains('Sm')
    <Condition: "author"."last_name" LIKE %s || %s || %s, [u'%', 'Sm', u'%']>

    >>> T.author.last_name.icontains('Sm')
    <Condition: "author"."last_name" ILIKE %s || %s || %s, [u'%', 'Sm', u'%']>

    >>> T.author.last_name.endswith('Sm')
    <Condition: "author"."last_name" LIKE %s || %s, [u'%', 'Sm']>

    >>> T.author.last_name.iendswith('Sm')
    <Condition: "author"."last_name" ILIKE %s || %s, [u'%', 'Sm']>

    >>> T.author.age.between(20, 30)
    <Between: "author"."age" BETWEEN %s AND %s, [20, 30]>


Django integration.
=====================

Simple add "sqlbuilder.django_sqlbuilder" to your INSTALLED_APPS.

::

    >>> object_list = Book.s.q.tables(
    ...     (Book.s & Author.s).on(Book.s.author == Author.s.pk)
    ... ).where(
    ...     (Author.s.first_name != 'James') & (Author.s.last_name != 'Joyce')
    ... )[:10]

