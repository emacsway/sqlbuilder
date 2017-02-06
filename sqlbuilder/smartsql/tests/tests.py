from __future__ import absolute_import
import datetime
import operator
from collections import OrderedDict
from sqlbuilder.smartsql.tests.base import TestCase

from sqlbuilder.smartsql import (
    PLACEHOLDER, Q, T, F, A, E, Not, func, const,
    FieldList, ExprList, Result, TableJoin, compile
)
from sqlbuilder.smartsql.dialects.mysql import compile as mysql_compile

__all__ = ('TestQuery', 'TestResult', 'TestSmartSQLLegacy',)


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
            TableJoin
        )
        self.assertEqual(
            compile(q.tables()),
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES (%s, %s, %s) ON CONFLICT DO UPDATE SET "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES %s, %s, %s ON CONFLICT DO UPDATE SET "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES (%s, %s, %s), (%s, %s, %s) ON CONFLICT DO UPDATE SET "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1, 'author', 16, 1])
        )
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=('author', 15, 1),
                ignore=True
            ),
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES %s, %s, %s ON CONFLICT DO NOTHING', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") SELECT "old_stats"."object_type", "old_stats"."object_id", "old_stats"."counter" FROM "old_stats" ON CONFLICT DO UPDATE SET "stats"."counter" = "stats"."counter" + "old_stats"."counter"', [])
        )

    def test_update(self):
        self.assertEqual(
            Q(T.author).where(T.author.id == 10).update(OrderedDict((
                (T.author.first_name, 'John'),
                (T.author.last_login, func.NOW()),
            ))),
            ('UPDATE "author" SET "author"."first_name" = %s, "author"."last_login" = NOW() WHERE "author"."id" = %s', ['John', 10])
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
            ('UPDATE "author" SET "author"."first_name" = %s, "author"."last_login" = NOW() WHERE "author"."id" = %s', ['John', 10])
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


class TestSmartSQLLegacy(TestCase):

    def test_prefix(self):
        self.assertEqual(
            Q(T.tb).where(~(T.tb.cl == 3)).select('*'),
            ('SELECT * FROM "tb" WHERE NOT "tb"."cl" = %s', [3, ], )
        )
        self.assertEqual(
            Q(T.tb).where(Not(T.tb.cl == 3)).select('*'),
            ('SELECT * FROM "tb" WHERE NOT "tb"."cl" = %s', [3, ], )
        )

    def test_function(self):
        self.assertEqual(
            Q(T.tb).where(func.FUNC_NAME(T.tb.cl) == 5).select('*'),
            ('SELECT * FROM "tb" WHERE FUNC_NAME("tb"."cl") = %s', [5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl == func.RAND()).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" = RAND()', [], )
        )

    def test_constant(self):
        self.assertEqual(
            Q(T.tb).where(const.CONST_NAME == 5).select('*'),
            ('SELECT * FROM "tb" WHERE CONST_NAME = %s', [5, ], )
        )

    def test_in(self):
        self.assertEqual(
            Q(T.tb).where(T.tb.cl == [1, T.tb.cl3, 5, ]).where(T.tb.cl2 == [1, T.tb.cl4, ]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" IN (%s, "tb"."cl3", %s) AND "tb"."cl2" IN (%s, "tb"."cl4")', [1, 5, 1, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl != [1, 3, 5, ]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" NOT IN (%s, %s, %s)', [1, 3, 5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl.in_([1, 3, 5, ])).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" IN (%s, %s, %s)', [1, 3, 5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl.not_in([1, 3, 5, ])).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" NOT IN (%s, %s, %s)', [1, 3, 5, ], )
        )

    def test_concat(self):
        self.assertEqual(
            Q(T.tb).where(T.tb.cl.concat(1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" || %s || %s || %s || "tb"."cl2" <> %s', [1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl.concat_ws(' + ', 1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM "tb" WHERE concat_ws(%s, "tb"."cl", %s, %s, %s, "tb"."cl2") <> %s', [' + ', 1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            Q(T.tb, result=Result(compile=mysql_compile)).where(T.tb.cl.concat(1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM `tb` WHERE CONCAT(`tb`.`cl`, %s, %s, %s, `tb`.`cl2`) <> %s', [1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            Q(T.tb, result=Result(compile=mysql_compile)).where(T.tb.cl.concat_ws(' + ', 1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM `tb` WHERE CONCAT_WS(%s, `tb`.`cl`, %s, %s, %s, `tb`.`cl2`) <> %s', [' + ', 1, 2, 'str', 'str2'], )
        )

    def test_alias(self):
        self.assertEqual(
            Q(T.tb).where(A('al') == 5).select(F.tb__cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE "al" = %s', [5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(A('al') == 5).select(T.tb.cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE "al" = %s', [5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(A('al') == 5).select(T.tb.cl.as_('al')),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE "al" = %s', [5, ], )
        )

    def test_complex(self):
        self.assertEqual(
            Q((T.base + T.grade).on((T.base.type == T.grade.item_type) & (F.base__type == 1)) + T.lottery).on(
                F.base__type == F.lottery__item_type
            ).where(
                (F.name == "name") & (F.status == 0) | (F.name == None)
            ).group_by(T.base.type).having(E("count(*)") > 1).select(F.type, F.grade__grade, F.lottery__grade),
            ('SELECT "type", "grade"."grade", "lottery"."grade" FROM "base" LEFT OUTER JOIN "grade" ON ("base"."type" = "grade"."item_type" AND "base"."type" = %s) LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE "name" = %s AND "status" = %s OR "name" IS NULL GROUP BY "base"."type" HAVING (count(*)) > %s', [1, 'name', 0, 1, ], )
        )
        t = T.grade
        self.assertEqual(
            Q(t).limit(0, 100).select(F.name),
            ('SELECT "name" FROM "grade" LIMIT %s', [100], )
        )
        t = (t & T.base).on(F.grade__item_type == F.base__type)
        self.assertEqual(
            Q(t).order_by(F.grade__name, F.base__name, desc=True).select(F.grade__name, F.base__img),
            ('SELECT "grade"."name", "base"."img" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") ORDER BY "grade"."name" DESC, "base"."name" DESC', [], )
        )
        t = (t + T.lottery).on(F.base__type == F.lottery__item_type)
        self.assertEqual(
            Q(t).group_by(F.grade__grade).having(F.grade__grade > 0).select(F.grade__name, F.base__img, F.lottery__price),
            ('SELECT "grade"."name", "base"."img", "lottery"."price" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") GROUP BY "grade"."grade" HAVING "grade"."grade" > %s', [0, ], )
        )
        w = (F.base__type == 1)
        self.assertEqual(
            Q(t).where(w).select(F.grade__name, for_update=True),
            ('SELECT "grade"."name" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE "base"."type" = %s FOR UPDATE', [1, ], )
        )
        w = w & (F.grade__status == [0, 1])
        now = datetime.datetime.now()
        w = w | (F.lottery__add_time > "2009-01-01") & (F.lottery__add_time <= now)
        self.assertEqual(
            Q(t).where(w).limit(1).select(F.grade__name, F.base__img, F.lottery__price),
            ('SELECT "grade"."name", "base"."img", "lottery"."price" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE "base"."type" = %s AND "grade"."status" IN (%s, %s) OR "lottery"."add_time" > %s AND "lottery"."add_time" <= %s LIMIT %s', [1, 0, 1, '2009-01-01', now, 1], )
        )
        w = w & (F.base__status != [1, 2])
        self.assertEqual(
            Q(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("CASE 1 WHEN 1")),
            ('SELECT "grade"."name", "base"."img", "lottery"."price", (CASE 1 WHEN 1) FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE ("base"."type" = %s AND "grade"."status" IN (%s, %s) OR "lottery"."add_time" > %s AND "lottery"."add_time" <= %s) AND "base"."status" NOT IN (%s, %s)', [1, 0, 1, '2009-01-01', now, 1, 2, ], )
        )
        self.assertEqual(
            Q(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("CASE 1 WHEN " + PLACEHOLDER, 'exp_value').as_("exp_result")),
            ('SELECT "grade"."name", "base"."img", "lottery"."price", (CASE 1 WHEN %s) AS "exp_result" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE ("base"."type" = %s AND "grade"."status" IN (%s, %s) OR "lottery"."add_time" > %s AND "lottery"."add_time" <= %s) AND "base"."status" NOT IN (%s, %s)', ['exp_value', 1, 0, 1, '2009-01-01', now, 1, 2, ], )
        )
        q = Q(T.user)
        self.assertEqual(
            q.select(F.name),
            ('SELECT "name" FROM "user"', [], )
        )
        q = q.tables((q.tables() & T.address).on(F.user__id == F.address__user_id))
        self.assertEqual(
            q.select(F.user__name, F.address__street),
            ('SELECT "user"."name", "address"."street" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id")', [], )
        )
        q = q.where(F.id == 1)
        self.assertEqual(
            q.select(F.name, F.id),
            ('SELECT "name", "id" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id") WHERE "id" = %s', [1, ], )
        )
        q = q.where((F.address__city_id == [111, 112]) | E("address.city_id IS NULL"))
        self.assertEqual(
            q.select(F.user__name, F.address__street, func.COUNT(F("*")).as_("count")),
            ('SELECT "user"."name", "address"."street", COUNT(*) AS "count" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id") WHERE "id" = %s AND ("address"."city_id" IN (%s, %s) OR (address.city_id IS NULL))', [1, 111, 112, ], )
        )

    def test_subquery(self):
        sub_q = Q(T.tb2).fields(T.tb2.id2).where(T.tb2.id == T.tb1.tb2_id).limit(1)
        self.assertEqual(
            Q(T.tb1).where(T.tb1.tb2_id == sub_q).select(T.tb1.id),
            ('SELECT "tb1"."id" FROM "tb1" WHERE "tb1"."tb2_id" = (SELECT "tb2"."id2" FROM "tb2" WHERE "tb2"."id" = "tb1"."tb2_id" LIMIT %s)', [1], )
        )
        self.assertEqual(
            Q(T.tb1).where(T.tb1.tb2_id.in_(sub_q)).select(T.tb1.id),
            ('SELECT "tb1"."id" FROM "tb1" WHERE "tb1"."tb2_id" IN (SELECT "tb2"."id2" FROM "tb2" WHERE "tb2"."id" = "tb1"."tb2_id" LIMIT %s)', [1], )
        )
        self.assertEqual(
            Q(T.tb1).select(sub_q.as_('sub_value')),
            ('SELECT (SELECT "tb2"."id2" FROM "tb2" WHERE "tb2"."id" = "tb1"."tb2_id" LIMIT %s) AS "sub_value" FROM "tb1"', [1], )
        )

    def test_expression(self):
        self.assertEqual(
            Q(T.tb1).select(E('5 * 3 - 2*8').as_('sub_value')),
            ('SELECT (5 * 3 - 2*8) AS "sub_value" FROM "tb1"', [], )
        )
        self.assertEqual(
            Q(T.tb1).select(E('(5 - 2) * 8 + (6 - 3) * 8').as_('sub_value')),
            ('SELECT ((5 - 2) * 8 + (6 - 3) * 8) AS "sub_value" FROM "tb1"', [], )
        )

    def test_union(self):
        a = Q(T.item).where(T.item.status != -1).fields(T.item.type, T.item.name, T.item.img)
        b = Q(T.gift).where(T.gift.storage > 0).columns(T.gift.type, T.gift.name, T.gift.img)
        self.assertEqual(
            (a.as_set(True) | b).order_by("type", "name", desc=True).limit(100, 10).select(),
            ('(SELECT "item"."type", "item"."name", "item"."img" FROM "item" WHERE "item"."status" <> %s) UNION ALL (SELECT "gift"."type", "gift"."name", "gift"."img" FROM "gift" WHERE "gift"."storage" > %s) ORDER BY %s DESC, %s DESC LIMIT %s OFFSET %s', [-1, 0, 'type', 'name', 10, 100], )
        )
