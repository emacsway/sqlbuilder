===========
SQLBuilder
===========

SmartSQL - lightweight sql builder, follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_, less than 50 Kb.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Autumn ORM <https://bitbucket.org/emacsway/autumn>`_.

Home Page: https://bitbucket.org/emacsway/sqlbuilder

LICENSE:

* License is BSD

Short manual for sqlbuilder.smartsql
=====================================

table::

    In [9]: T.book
    Out[9]: <Table: "book", []>

    In [10]: T.book__a
    Out[10]: <TableAlias: "a", []>

    In [11]: T.book.as_('a')
    Out[11]: <TableAlias: "a", []>

field::

    In [13]: T.book.name
    Out[13]: <Field: "book"."name", []>

    In [14]: T.book.name.as_('a')
    Out[14]: <Alias: "a", []>

    In [15]: F.book__name
    Out[15]: <Field: "book"."name", []>

    In [16]: F.book__name__a
    Out[16]: <Alias: "a", []>

    In [17]: F.book__name.as_('a')
    Out[17]: <Alias: "a", []>


table operator::

    In [4]: (T.book & T.author).on(T.book.author_id == T.author.id)
    Out[4]: <TableJoin: "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    In [5]: (T.book + T.author).on(T.book.author_id == T.author.id)
    Out[5]: <TableJoin: "book" LEFT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    In [6]: (T.book - T.author).on(T.book.author_id == T.author.id)
    Out[6]: <TableJoin: "book" RIGHT OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    In [7]: (T.book | T.author).on(T.book.author_id == T.author.id)
    Out[7]: <TableJoin: "book" FULL OUTER JOIN "author" ON ("book"."author_id" = "author"."id"), []>

    In [8]: (T.book * T.author).on(T.book.author_id == T.author.id)
    Out[8]: <TableJoin: "book" CROSS JOIN "author" ON ("book"."author_id" = "author"."id"), []>

condition operator::

    In [19]: (T.author.first_name != 'Tom') & (T.author.last_name.in_(('Smith', 'Johnson')))
    Out[19]: <Condition: ("author"."first_name" <> %s) AND ("author"."last_name" IN (%s, %s)), ['Tom', 'Smith', 'Johnson']>

    In [20]: (T.author.first_name != 'Tom') | (T.author.last_name.in_(('Smith', 'Johnson')))
    Out[20]: <Condition: ("author"."first_name" <> %s) OR ("author"."last_name" IN (%s, %s)), ['Tom', 'Smith', 'Johnson']>

    In [21]: T.author.last_name.startswith('Sm')
    Out[21]: <Condition: "author"."last_name" LIKE %s || %s, ['Sm', u'%']>

    In [22]: T.author.last_name.istartswith('Sm')
    Out[22]: <Condition: "author"."last_name" ILIKE %s || %s, ['Sm', u'%']>

    In [23]: T.author.last_name.contains('Sm')
    Out[23]: <Condition: "author"."last_name" LIKE %s || %s || %s, [u'%', 'Sm', u'%']>

    In [24]: T.author.last_name.icontains('Sm')
    Out[24]: <Condition: "author"."last_name" ILIKE %s || %s || %s, [u'%', 'Sm', u'%']>

    In [25]: T.author.last_name.endswith('Sm')
    Out[25]: <Condition: "author"."last_name" LIKE %s || %s, [u'%', 'Sm']>

    In [26]: T.author.last_name.iendswith('Sm')
    Out[26]: <Condition: "author"."last_name" ILIKE %s || %s, [u'%', 'Sm']>

    In [27]: T.author.age.between(20, 30)
    Out[27]: <Between: "author"."age" BETWEEN %s AND %s, [20, 30]>


usage eg::

    In [31]: QS().tables(
        (T.book & T.author).on(T.book.author_id == T.author.id)
    ).columns(
        T.book.name, T.author.first_name, T.author.last_name
    ).where(
        (T.author.first_name != 'Tom') & (T.author.last_name != 'Smith')
    )[20:30]
    Out[31]: <QuerySet: SELECT "book"."name", "author"."first_name", "author"."last_name" FROM "book" INNER JOIN "author" ON ("book"."author_id" = "author"."id") WHERE (("author"."first_name" <> %s) AND ("author"."last_name" <> %s)) LIMIT 10 OFFSET 20, ['Tom', 'Smith']>



Django integration.
=====================

Simple add "sqlbuilder.django_sqlbuilder" to your INSTALLED_APPS.

::

    object_list = Book.s.qs.tables(
        (Book.s & Author.s).on(Book.s.author == Author.s.pk)
    ).where(
        (Author.s.first_name != 'James') & (Author.s.last_name != 'Joyce')
    )[:10]

