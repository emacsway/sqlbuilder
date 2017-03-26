import operator
from collections import OrderedDict
from sqlbuilder.smartsql.tests.base import TestCase

from sqlbuilder.smartsql import Q, T, func, FieldList, ExprList, Result, TableAlias, TableJoin, compile
from sqlbuilder.smartsql.dialects.mysql import compile as mysql_compile

__all__ = ('TestQuery', 'TestResult', )


class TestQuery(TestCase):

    def test_distinct(self):
        q = Q().tables(T.author).fields(T.author.first_name, T.author.last_name, T.author.age)
        self.assertEqual(
            compile(q),
            ('SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        q = q.distinct(T.author.first_name, T.author.last_name)
        self.assertEqual(
            compile(q),
            ('SELECT DISTINCT ON ("author"."first_name", "author"."last_name") "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        q = q.distinct(T.author.age)
        self.assertEqual(
            compile(q),
            ('SELECT DISTINCT ON ("author"."first_name", "author"."last_name", "author"."age") "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        self.assertEqual(
            type(q.distinct()),
            ExprList
        )
        self.assertEqual(
            compile(q.distinct()),
            ('"author"."first_name", "author"."last_name", "author"."age"', [])
        )
        self.assertEqual(
            compile(q.distinct([T.author.id, T.author.status])),
            ('SELECT DISTINCT ON ("author"."id", "author"."status") "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        self.assertEqual(
            compile(q.distinct([])),
            ('SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        self.assertEqual(
            compile(q.distinct(reset=True)),
            ('SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT DISTINCT ON ("author"."first_name", "author"."last_name", "author"."age") "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )

    def test_distinct_bool(self):
        q = Q().fields('*').tables(T.author)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author"', [])
        )
        self.assertFalse(
            q.distinct()
        )
        q = q.distinct(True)
        self.assertEqual(
            compile(q),
            ('SELECT DISTINCT * FROM "author"', [])
        )
        self.assertTrue(
            q.distinct()[0]
        )
        self.assertEqual(
            compile(q.distinct(False)),
            ('SELECT * FROM "author"', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT DISTINCT * FROM "author"', [])
        )

    def test_fields(self):
        q = Q().tables(T.author)
        q = q.fields(T.author.first_name, T.author.last_name)
        self.assertEqual(
            compile(q),
            ('SELECT "author"."first_name", "author"."last_name" FROM "author"', [])
        )
        q = q.fields(T.author.age)
        self.assertEqual(
            compile(q),
            ('SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )
        self.assertEqual(
            type(q.fields()),
            FieldList
        )
        self.assertEqual(
            compile(q.fields()),
            ('"author"."first_name", "author"."last_name", "author"."age"', [])
        )
        self.assertEqual(
            compile(q.fields([T.author.id, T.author.status])),
            ('SELECT "author"."id", "author"."status" FROM "author"', [])
        )
        self.assertEqual(
            compile(q.fields([])),
            ('SELECT  FROM "author"', [])
        )
        self.assertEqual(
            compile(q.fields(reset=True)),
            ('SELECT  FROM "author"', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT "author"."first_name", "author"."last_name", "author"."age" FROM "author"', [])
        )

    def test_tables(self):
        q = Q().tables(T.author).fields('*')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author"', [])
        )
        q = q.tables(T.author.as_('author_alias'))
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" AS "author_alias"', [])
        )
        self.assertEqual(
            type(q.tables()),
            TableAlias
        )
        self.assertEqual(
            compile(TableJoin(q.tables())),
            ('"author" AS "author_alias"', [])
        )
        self.assertEqual(
            compile(q.tables((q.tables() + T.book).on(T.book.author_id == T.author.as_('author_alias').id))),
            ('SELECT * FROM "author" AS "author_alias" LEFT OUTER JOIN "book" ON ("book"."author_id" = "author_alias"."id")', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" AS "author_alias"', [])
        )

    def test_where(self):
        q = Q().tables(T.author).fields('*')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author"', [])
        )
        q = q.where(T.author.is_staff.is_(True))
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" WHERE "author"."is_staff" IS %s', [True])
        )
        q = q.where(T.author.first_name == 'John')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" WHERE "author"."is_staff" IS %s AND "author"."first_name" = %s', [True, 'John'])
        )
        q = q.where(T.author.last_name == 'Smith', op=operator.or_)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" WHERE "author"."is_staff" IS %s AND "author"."first_name" = %s OR "author"."last_name" = %s', [True, 'John', 'Smith'])
        )
        q = q.where(T.author.last_name == 'Smith', op=None)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" WHERE "author"."last_name" = %s', ['Smith'])
        )

    def test_group_by(self):
        q = Q().tables(T.author).fields('*')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author"', [])
        )
        q = q.group_by(T.author.first_name, T.author.last_name)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."first_name", "author"."last_name"', [])
        )
        q = q.group_by(T.author.age)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."first_name", "author"."last_name", "author"."age"', [])
        )
        self.assertEqual(
            type(q.group_by()),
            ExprList
        )
        self.assertEqual(
            compile(q.group_by()),
            ('"author"."first_name", "author"."last_name", "author"."age"', [])
        )
        self.assertEqual(
            compile(q.group_by([T.author.id, T.author.status])),
            ('SELECT * FROM "author" GROUP BY "author"."id", "author"."status"', [])
        )
        self.assertEqual(
            compile(q.group_by([])),
            ('SELECT * FROM "author"', [])
        )
        self.assertEqual(
            compile(q.group_by(reset=True)),
            ('SELECT * FROM "author"', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."first_name", "author"."last_name", "author"."age"', [])
        )

    def test_having(self):
        q = Q().fields('*').tables(T.author).group_by(T.author.status)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."status"', [])
        )
        q = q.having(T.author.is_staff.is_(True))
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."status" HAVING "author"."is_staff" IS %s', [True])
        )
        q = q.having(T.author.first_name == 'John')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."status" HAVING "author"."is_staff" IS %s AND "author"."first_name" = %s', [True, 'John'])
        )
        q = q.having(T.author.last_name == 'Smith', op=operator.or_)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."status" HAVING "author"."is_staff" IS %s AND "author"."first_name" = %s OR "author"."last_name" = %s', [True, 'John', 'Smith'])
        )
        q = q.having(T.author.last_name == 'Smith', op=None)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" GROUP BY "author"."status" HAVING "author"."last_name" = %s', ['Smith'])
        )

    def test_order_by(self):
        q = Q().tables(T.author).fields('*')
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author"', [])
        )
        q = q.order_by(T.author.first_name, T.author.last_name)
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" ORDER BY "author"."first_name" ASC, "author"."last_name" ASC', [])
        )
        q = q.order_by(T.author.age.desc())
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" ORDER BY "author"."first_name" ASC, "author"."last_name" ASC, "author"."age" DESC', [])
        )
        self.assertEqual(
            type(q.order_by()),
            ExprList
        )
        self.assertEqual(
            compile(q.order_by()),
            ('"author"."first_name" ASC, "author"."last_name" ASC, "author"."age" DESC', [])
        )
        self.assertEqual(
            compile(q.order_by([T.author.id.desc(), T.author.status])),
            ('SELECT * FROM "author" ORDER BY "author"."id" DESC, "author"."status" ASC', [])
        )
        self.assertEqual(
            compile(q.order_by([])),
            ('SELECT * FROM "author"', [])
        )
        self.assertEqual(
            compile(q.order_by(reset=True)),
            ('SELECT * FROM "author"', [])
        )
        self.assertEqual(
            compile(q),
            ('SELECT * FROM "author" ORDER BY "author"."first_name" ASC, "author"."last_name" ASC, "author"."age" DESC', [])
        )

    def test_select(self):
        q = Q(T.author).fields('*')
        self.assertEqual(
            q.select(for_update=True),
            ('SELECT * FROM "author" FOR UPDATE', [])
        )

    def test_count(self):
        q = Q(T.author).fields('*')
        self.assertEqual(
            q.count(),
            ('SELECT COUNT(1) AS "count_value" FROM (SELECT * FROM "author") AS "count_list"', [])
        )

    def test_insert(self):
        self.assertEqual(
            Q(T.stats).insert(OrderedDict((
                (T.stats.object_type, 'author'),
                (T.stats.object_id, 15),
                (T.stats.counter, 1),
            )), on_duplicate_key_update=OrderedDict((
                (T.stats.counter, T.stats.counter + func.VALUES(T.stats.counter)),
            ))),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES (%s, %s, %s) ON CONFLICT DO UPDATE SET "counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
        )
        self.assertEqual(
            Q(T.stats).insert(OrderedDict((
                ('object_type', 'author'),
                ('object_id', 15),
                ('counter', 1),
            )), on_duplicate_key_update=OrderedDict((
                ('counter', T.stats.counter + func.VALUES(T.stats.counter)),
            ))),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES (%s, %s, %s) ON CONFLICT DO UPDATE SET "counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
        )
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=('author', 15, 1),
                on_duplicate_key_update=OrderedDict((
                    (T.stats.counter, T.stats.counter + func.VALUES(T.stats.counter)),
                ))
            ),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES %s, %s, %s ON CONFLICT DO UPDATE SET "counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
        )
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=(
                    ('author', 15, 1),
                    ('author', 16, 1),
                ),
                on_duplicate_key_update=OrderedDict((
                    (T.stats.counter, T.stats.counter + func.VALUES(T.stats.counter)),
                ))
            ),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES (%s, %s, %s), (%s, %s, %s) ON CONFLICT DO UPDATE SET "counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1, 'author', 16, 1])
        )
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=('author', 15, 1),
                ignore=True
            ),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES %s, %s, %s ON CONFLICT DO NOTHING', ['author', 15, 1])
        )

    def test_insert_select(self):
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=Q().fields(
                    T.old_stats.object_type, T.old_stats.object_id, T.old_stats.counter
                ).tables(T.old_stats),
                on_duplicate_key_update=OrderedDict((
                    (T.stats.counter, T.stats.counter + T.old_stats.counter),
                ))
            ),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") SELECT "old_stats"."object_type", "old_stats"."object_id", "old_stats"."counter" FROM "old_stats" ON CONFLICT DO UPDATE SET "counter" = "stats"."counter" + "old_stats"."counter"', [])
        )

    def test_update(self):
        self.assertEqual(
            Q(T.author).where(T.author.id == 10).update(OrderedDict((
                (T.author.first_name, 'John'),
                (T.author.last_login, func.NOW()),
            ))),
            ('UPDATE "author" SET "first_name" = %s, "last_login" = NOW() WHERE "author"."id" = %s', ['John', 10])
        )
        self.assertEqual(
            Q(T.author).where(T.author.id == 10).update(OrderedDict((
                ('first_name', 'John'),
                ('last_login', func.NOW()),
            ))),
            ('UPDATE "author" SET "first_name" = %s, "last_login" = NOW() WHERE "author"."id" = %s', ['John', 10])
        )
        self.assertEqual(
            Q(T.author).fields(
                T.author.first_name, T.author.last_login
            ).where(T.author.id == 10).update(
                values=('John', func.NOW())
            ),
            ('UPDATE "author" SET "first_name" = %s, "last_login" = NOW() WHERE "author"."id" = %s', ['John', 10])
        )

    def test_delete(self):
        self.assertEqual(
            Q(T.author).where(T.author.id == 10).delete(),
            ('DELETE FROM "author" WHERE "author"."id" = %s', [10])
        )

    def test_as_table(self):
        author_query_alias = Q(T.author).fields(T.author.id).where(T.author.status == 'active').as_table('author_query_alias')
        self.assertEqual(
            compile(Q().fields(T.book.id, T.book.title).tables((T.book & author_query_alias).on(T.book.author_id == author_query_alias.id))),
            ('SELECT "book"."id", "book"."title" FROM "book" INNER JOIN (SELECT "author"."id" FROM "author" WHERE "author"."status" = %s) AS "author_query_alias" ON ("book"."author_id" = "author_query_alias"."id")', ['active'])
        )

    def test_set(self):
        q1 = Q(T.book1).fields(T.book1.id, T.book1.title).where(T.book1.author_id == 10)
        q2 = Q(T.book2).fields(T.book2.id, T.book2.title).where(T.book2.author_id == 10)
        self.assertEqual(
            compile(q1.as_set() | q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) UNION (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )
        self.assertEqual(
            compile(q1.as_set() & q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) INTERSECT (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )
        self.assertEqual(
            compile(q1.as_set() - q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) EXCEPT (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )
        self.assertEqual(
            compile(q1.as_set(all=True) | q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) UNION ALL (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )
        self.assertEqual(
            compile(q1.as_set(all=True) & q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) INTERSECT ALL (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )
        self.assertEqual(
            compile(q1.as_set(all=True) - q2),
            ('(SELECT "book1"."id", "book1"."title" FROM "book1" WHERE "book1"."author_id" = %s) EXCEPT ALL (SELECT "book2"."id", "book2"."title" FROM "book2" WHERE "book2"."author_id" = %s)', [10, 10])
        )

    def test_where_subquery(self):
        sub_q = Q().fields(T.author.id).tables(T.author).where(T.author.status == 'active')
        q = Q().fields(T.book.id).tables(T.book).where(T.book.author_id.in_(sub_q))
        self.assertEqual(
            compile(q),
            ('SELECT "book"."id" FROM "book" WHERE "book"."author_id" IN (SELECT "author"."id" FROM "author" WHERE "author"."status" = %s)', ['active'])
        )

    def test_fields_subquery(self):
        sub_q = Q().fields(T.book.id.count().as_("book_count")).tables(T.book).where(T.book.pub_date > '2015-01-01').group_by(T.book.author_id)
        q = Q().fields(T.author.id, sub_q.where(T.book.author_id == T.author.id)).tables(T.author).where(T.author.status == 'active')
        self.assertEqual(
            compile(q),
            ('SELECT "author"."id", (SELECT COUNT("book"."id") AS "book_count" FROM "book" WHERE "book"."pub_date" > %s AND "book"."author_id" = "author"."id" GROUP BY "book"."author_id") FROM "author" WHERE "author"."status" = %s', ['2015-01-01', 'active'])
        )

    def test_alias_subquery(self):
        alias = Q().fields(T.book.id.count()).tables(T.book).where((T.book.pub_date > '2015-01-01') & (T.book.author_id == T.author.id)).group_by(T.book.author_id).as_("book_count")
        q = Q().fields(T.author.id, alias).tables(T.author).where(T.author.status == 'active').order_by(alias.desc())
        self.assertEqual(
            compile(q),
            ('SELECT "author"."id", (SELECT COUNT("book"."id") FROM "book" WHERE "book"."pub_date" > %s AND "book"."author_id" = "author"."id" GROUP BY "book"."author_id") AS "book_count" FROM "author" WHERE "author"."status" = %s ORDER BY "book_count" DESC', ['2015-01-01', 'active'])
        )


class TestResult(TestCase):

    def test_result(self):

        class CustomResult(Result):

            custom_attr = 5

            def custom_method(self, arg1, arg2):
                return (self._query, arg1, arg2)

            def find_by_name(self, name):
                return self._query.where(T.author.name == name)

        q = Q(result=CustomResult(compile=mysql_compile)).fields(T.author.id, T.author.name).tables(T.author)

        self.assertEqual(q.custom_attr, 5)
        q2, arg1, arg2 = q.custom_method(5, 10)
        self.assertIsNot(q2, q)
        self.assertEqual(q2.select(), q.select())
        self.assertEqual(q2.select(), ('SELECT `author`.`id`, `author`.`name` FROM `author`', []))
        self.assertEqual(arg1, 5)
        self.assertEqual(arg2, 10)

        q3 = q.find_by_name('John')
        self.assertIsNot(q3, q)
        self.assertEqual(q3.select(), ('SELECT `author`.`id`, `author`.`name` FROM `author` WHERE `author`.`name` = %s', ['John']))
