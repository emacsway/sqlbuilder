from __future__ import absolute_import, unicode_literals
from django.conf import settings
from django.db import models
from django.test import TestCase, override_settings
from sqlbuilder.smartsql import Table, TableAlias, Field, Query, compile


class Author(models.Model):
    first_name = models.CharField(max_length=255, blank=True)
    last_name = models.CharField(max_length=255, blank=True)

    class Meta:
        db_table = 'sqlbuilder_author'


class Book(models.Model):
    title = models.CharField(max_length=255, blank=True)
    author = models.ForeignKey(Author, blank=True, null=True)

    class Meta:
        db_table = 'sqlbuilder_book'


class TestDjangoSqlbuilder(TestCase):

    def test_table(self):
        table = Book.s
        self.assertIsInstance(table, Table)
        self.assertIsInstance(table.pk, Field)
        self.assertIsInstance(table.title, Field)
        self.assertIsInstance(table.author, Field)
        self.assertEqual(
            compile(table.pk),
            ('"sqlbuilder_book"."id"', [])
        )
        self.assertEqual(
            compile(table.title),
            ('"sqlbuilder_book"."title"', [])
        )
        self.assertEqual(
            compile(table.author),
            ('"sqlbuilder_book"."author_id"', [])
        )

    def test_tablealias(self):
        table = Book.s.as_('book_alias')
        self.assertIsInstance(table, TableAlias)
        self.assertIsInstance(table.pk, Field)
        self.assertIsInstance(table.title, Field)
        self.assertIsInstance(table.author, Field)
        self.assertEqual(
            compile(table.pk),
            ('"book_alias"."id"', [])
        )
        self.assertEqual(
            compile(table.title),
            ('"book_alias"."title"', [])
        )
        self.assertEqual(
            compile(table.author),
            ('"book_alias"."author_id"', [])
        )

    @override_settings(DEBUG=True)
    def test_query(self):
        author, book = self._create_objects()
        self.assertIsInstance(Book.s.q, Query)
        q = Book.s.q.where(Book.s.pk == book.id)
        self.assertEqual(
            compile(q),
            ('SELECT "sqlbuilder_book"."id", "sqlbuilder_book"."title", "sqlbuilder_book"."author_id" FROM "sqlbuilder_book" WHERE "sqlbuilder_book"."id" = %s', [book.id])
        )
        book2 = q[0]
        self.assertEqual(book2.id, book.id)

    def _create_objects(self):
        author = Author.objects.create(first_name='John', last_name='Smith')
        book = Book.objects.create(title="Title 1", author_id=author.id)
        return (author, book)
