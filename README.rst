===========
SQLBuilder
===========

SmartSQL - lightweight Python sql builder, follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_. Supports Python2 and Python3.

You can use SmartSQL separatelly, or with Django, or with super-lightweight `Ascetic ORM <https://bitbucket.org/emacsway/ascetic>`_, or with super-lightweight datamapper `Openorm <http://code.google.com/p/openorm/source/browse/python/>`_ (`miror <https://bitbucket.org/emacsway/openorm/src/default/python/>`__) etc.

* Home Page: https://bitbucket.org/emacsway/sqlbuilder
* Docs: http://sqlbuilder.readthedocs.io/
* Browse source code: https://bitbucket.org/emacsway/sqlbuilder/src
* Get source code: ``hg clone https://bitbucket.org/emacsway/sqlbuilder``
* PyPI: https://pypi.python.org/pypi/sqlbuilder

LICENSE:

* License is BSD


Quick start
===========

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


Django integration
==================

Simple add "django_sqlbuilder" to your INSTALLED_APPS.

::

    >>> object_list = Book.s.q.tables(
    ...     (Book.s & Author.s).on(Book.s.author == Author.s.pk)
    ... ).where(
    ...     (Author.s.first_name != 'James') & (Author.s.last_name != 'Joyce')
    ... )[:10]


More info
=========

See docs on http://sqlbuilder.readthedocs.io/

..

P.S.: See also `article (in Russian) about SQLBuilder <https://emacsway.bitbucket.io/ru/storm-orm/#query-object>`__.
