from __future__ import absolute_import
import datetime
import operator
import unittest
from collections import OrderedDict

from sqlbuilder.smartsql import (
    PLACEHOLDER, Q, T, Table, TA, F, Field, A, E, P, Not, func, const, CompositeExpr,
    Case, FieldList, ExprList, Result, TableJoin, compile
)
from sqlbuilder.smartsql.compilers.mysql import compile as mysql_compile

__all__ = ('TestTable', 'TestField', 'TestExpr', 'TestCaseExpr', 'TestQuery', 'TestResult', 'TestSmartSQLLegacy',)


class TestCase(unittest.TestCase):

    maxDiff = None


class TestTable(TestCase):

    def test_table(self):
        self.assertEqual(
            type(T.book),
            Table
        )
        self.assertEqual(
            compile(T.book),
            ('"book"', [])
        )
        self.assertEqual(
            type(T.book__a),
            TA
        )
        self.assertEqual(
            compile(T.book__a),
            ('"a"', [])
        )
        self.assertEqual(
            type(T.book.as_('a')),
            TA
        )
        self.assertEqual(
            compile(T.book.as_('a')),
            ('"a"', [])
        )
        ta = T.book.as_('a')
        self.assertEqual(
            compile(Q().tables(ta).fields(ta.id, ta.status).where(ta.status.in_(('new', 'approved')))),
            ('SELECT "a"."id", "a"."status" FROM "book" AS "a" WHERE "a"."status" IN (%s, %s)', ['new', 'approved'])
        )
        t = T.book
        self.assertIs(t.status, t.status)
        self.assertIs(t.status, t.f.status)
        self.assertIs(t.status, t.f('status'))
        self.assertIs(t.status, t.f['status'])
        self.assertIs(t.status, t['status'])
        self.assertIs(t.status, t.__getattr__('status'))
        self.assertIs(t.status, t.get_field('status'))

    def test_join(self):
        self.assertEqual(
            compile((T.book & T.author).on(T.book.author_id == T.author.id)),
            ('"book" INNER JOIN "author" ON ("book"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile((T.book + T.author).on(T.book.author_id == T.author.id)),
            ('"book" LEFT OUTER JOIN "author" ON ("book"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile((T.book - T.author).on(T.book.author_id == T.author.id)),
            ('"book" RIGHT OUTER JOIN "author" ON ("book"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile((T.book | T.author).on(T.book.author_id == T.author.id)),
            ('"book" FULL OUTER JOIN "author" ON ("book"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile((T.book * T.author).on(T.book.author_id == T.author.id)),
            ('"book" CROSS JOIN "author" ON ("book"."author_id" = "author"."id")', [])
        )

    def test_join_priorities(self):
        t1, t2, t3, t4, t5 = T.t1, T.t2, T.t3, T.t4, T.t5
        self.assertEqual(
            compile(t1 | t2.on(t2.t1_id == t1.id) * t3.on(t3.t1_id == t1.id) + t4.on(t4.t1_id == t1.id) - t5.on(t5.t1_id == t5.id)),
            ('"t1" FULL OUTER JOIN "t2" ON ("t2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t1_id" = "t1"."id") LEFT OUTER JOIN "t4" ON ("t4"."t1_id" = "t1"."id") RIGHT OUTER JOIN "t5" ON ("t5"."t1_id" = "t5"."id")', [])
        )
        self.assertEqual(
            compile(((((t1 | t2).on(t2.t1_id == t1.id) * t3).on(t3.t1_id == t1.id) + t4).on(t4.t1_id == t1.id) - t5.on(t5.t1_id == t5.id))),
            ('"t1" FULL OUTER JOIN "t2" ON ("t2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t1_id" = "t1"."id") LEFT OUTER JOIN "t4" ON ("t4"."t1_id" = "t1"."id") RIGHT OUTER JOIN "t5" ON ("t5"."t1_id" = "t5"."id")', [])
        )

    def test_join_nested(self):
        t1, t2, t3, t4 = T.t1, T.t2, T.t3, T.t4
        self.assertEqual(
            compile(t1 + (t2 * t3 * t4)().on((t2.a == t1.a) & (t3.b == t1.b) & (t4.c == t1.c))),
            ('"t1" LEFT OUTER JOIN ("t2" CROSS JOIN "t3" CROSS JOIN "t4") ON ("t2"."a" = "t1"."a" AND "t3"."b" = "t1"."b" AND "t4"."c" = "t1"."c")', [])
        )
        self.assertEqual(
            compile((t1 + (t2 * t3 * t4)()).on((t2.a == t1.a) & (t3.b == t1.b) & (t4.c == t1.c))),
            ('"t1" LEFT OUTER JOIN ("t2" CROSS JOIN "t3" CROSS JOIN "t4") ON ("t2"."a" = "t1"."a" AND "t3"."b" = "t1"."b" AND "t4"."c" = "t1"."c")', [])
        )
        self.assertEqual(
            compile(t1 + (t2 + t3).on((t2.b == t3.b) | t2.b.is_(None))()),
            ('"t1" LEFT OUTER JOIN ("t2" LEFT OUTER JOIN "t3" ON ("t2"."b" = "t3"."b" OR "t2"."b" IS NULL))', [])
        )
        self.assertEqual(
            compile((t1 + t2.on(t1.a == t2.a))() + t3.on((t2.b == t3.b) | t2.b.is_(None))),
            ('("t1" LEFT OUTER JOIN "t2" ON ("t1"."a" = "t2"."a")) LEFT OUTER JOIN "t3" ON ("t2"."b" = "t3"."b" OR "t2"."b" IS NULL)', [])
        )

    def test_join_nested_old(self):
        t1, t2, t3, t4 = T.t1, T.t2.as_('al2'), T.t3, T.t4
        self.assertEqual(
            Q((t1 + t2.on(t2.t1_id == t1.id)) * t3.on(t3.t2_id == t2.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            Q((t1 + t2).on(t2.t1_id == t1.id) * t3.on(t3.t2_id == t2.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            Q((t1 + ((t2 * t3).on(t3.t2_id == t2.id))()).on(t2.t1_id == t1.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN ("t2" AS "al2" CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id")) ON ("al2"."t1_id" = "t1"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            Q(((t1 + t2) * t3 - t4)().on((t2.t1_id == t1.id) & (t3.t2_id == t2.id) & (t4.t3_id == t3.id))).select(t1.id),
            ('SELECT "t1"."id" FROM ("t1" LEFT OUTER JOIN "t2" AS "al2" CROSS JOIN "t3" RIGHT OUTER JOIN "t4") ON ("al2"."t1_id" = "t1"."id" AND "t3"."t2_id" = "al2"."id" AND "t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            Q((t1 & t2.on(t2.t1_id == t1.id) & (t3 & t4.on(t4.t3_id == t3.id))()).on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )
        self.assertEqual(
            Q(t1 & t2.on(t2.t1_id == t1.id) & (t3 & t4.on(t4.t3_id == t3.id)).as_nested().on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )
        self.assertEqual(
            Q((t1 & t2.on(t2.t1_id == t1.id))() & (t3 & t4.on(t4.t3_id == t3.id))().on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM ("t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id")) INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )

    def test_hint(self):
        t1, t2 = T.tb1, T.tb1.as_('al2')
        q = Q(t1 & t2.hint(E('USE INDEX (`index1`, `index2`)')).on(t2.parent_id == t1.id))
        q.result.compile = mysql_compile
        self.assertEqual(
            q.select(t2.id),
            ('SELECT `al2`.`id` FROM `tb1` INNER JOIN `tb1` AS `al2` ON (`al2`.`parent_id` = `tb1`.`id`) USE INDEX (`index1`, `index2`)', [], )
        )


class TestField(TestCase):

    def test_field(self):

        # Get field as table attribute
        self.assertEqual(
            type(T.book.title),
            Field
        )
        self.assertEqual(
            compile(T.book.title),
            ('"book"."title"', [])
        )

        self.assertEqual(
            type(T.book.title.as_('a')),
            A
        )
        self.assertEqual(
            compile(T.book.title.as_('a')),
            ('"a"', [])
        )

        self.assertEqual(
            type(T.book.title__a),
            A
        )
        self.assertEqual(
            compile(T.book.title__a),
            ('"a"', [])
        )

        # Get field as class F attribute (Legacy)
        self.assertEqual(
            type(F.book__title),
            Field
        )
        self.assertEqual(
            compile(F.book__title),
            ('"book"."title"', [])
        )

        self.assertEqual(
            type(F.book__title.as_('a')),
            A
        )
        self.assertEqual(
            compile(F.book__title.as_('a')),
            ('"a"', [])
        )

        self.assertEqual(
            type(F.book__title__a),
            A
        )
        self.assertEqual(
            compile(F.book__title__a),
            ('"a"', [])
        )

        # Test with context
        al = T.book.status.as_('a')
        self.assertEqual(
            compile(Q().tables(T.book).fields(T.book.id, al).where(al.in_(('new', 'approved')))),
            ('SELECT "book"."id", "book"."status" AS "a" FROM "book" WHERE "a" IN (%s, %s)', ['new', 'approved'])
        )


class TestExpr(TestCase):

    def test_expr(self):
        tb = T.author
        self.assertEqual(
            compile(tb.name == 'Tom'),
            (('"author"."name" = %s'), ['Tom'])
        )
        self.assertEqual(
            compile(tb.name != 'Tom'),
            (('"author"."name" <> %s'), ['Tom'])
        )
        self.assertEqual(
            compile(tb.counter + 1),
            ('"author"."counter" + %s', [1])
        )
        self.assertEqual(
            compile(1 + tb.counter),
            ('%s + "author"."counter"', [1])
        )
        self.assertEqual(
            compile(tb.counter - 1),
            ('"author"."counter" - %s', [1])
        )
        self.assertEqual(
            compile(10 - tb.counter),
            ('%s - "author"."counter"', [10])
        )
        self.assertEqual(
            compile(tb.counter * 2),
            ('"author"."counter" * %s', [2])
        )
        self.assertEqual(
            compile(2 * tb.counter),
            ('%s * "author"."counter"', [2])
        )
        self.assertEqual(
            compile(tb.counter / 2),
            ('"author"."counter" / %s', [2])
        )
        self.assertEqual(
            compile(10 / tb.counter),
            ('%s / "author"."counter"', [10])
        )
        self.assertEqual(
            compile(tb.is_staff & tb.is_admin),
            ('"author"."is_staff" AND "author"."is_admin"', [])
        )
        self.assertEqual(
            compile(tb.is_staff | tb.is_admin),
            ('"author"."is_staff" OR "author"."is_admin"', [])
        )
        self.assertEqual(
            compile(tb.counter > 10),
            ('"author"."counter" > %s', [10])
        )
        self.assertEqual(
            compile(10 > tb.counter),
            ('"author"."counter" < %s', [10])
        )
        self.assertEqual(
            compile(tb.counter >= 10),
            ('"author"."counter" >= %s', [10])
        )
        self.assertEqual(
            compile(10 >= tb.counter),
            ('"author"."counter" <= %s', [10])
        )
        self.assertEqual(
            compile(tb.counter < 10),
            ('"author"."counter" < %s', [10])
        )
        self.assertEqual(
            compile(10 < tb.counter),
            ('"author"."counter" > %s', [10])
        )
        self.assertEqual(
            compile(tb.counter <= 10),
            ('"author"."counter" <= %s', [10])
        )
        self.assertEqual(
            compile(10 <= tb.counter),
            ('"author"."counter" >= %s', [10])
        )
        self.assertEqual(
            compile(tb.mask << 1),
            ('"author"."mask" << %s', [1])
        )
        self.assertEqual(
            compile(tb.mask >> 1),
            ('"author"."mask" >> %s', [1])
        )
        self.assertEqual(
            compile(tb.is_staff.is_(True)),
            ('"author"."is_staff" IS %s', [True])
        )
        self.assertEqual(
            compile(tb.is_staff.is_not(True)),
            ('"author"."is_staff" IS NOT %s', [True])
        )
        self.assertEqual(
            compile(tb.status.in_(('new', 'approved'))),
            ('"author"."status" IN (%s, %s)', ['new', 'approved'])
        )
        self.assertEqual(
            compile(tb.status.not_in(('new', 'approved'))),
            ('"author"."status" NOT IN (%s, %s)', ['new', 'approved'])
        )
        self.assertEqual(
            compile(tb.last_name.like('mi')),
            ('"author"."last_name" LIKE %s', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.ilike('mi')),
            ('"author"."last_name" ILIKE %s', ['mi'])
        )
        self.assertEqual(
            compile(P('mi').like(tb.last_name)),
            ('%s LIKE "author"."last_name"', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.rlike('mi')),
            ('%s LIKE "author"."last_name"', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.rilike('mi')),
            ('%s ILIKE "author"."last_name"', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.startswith('Sm')),
            ('"author"."last_name" LIKE REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['Sm'])
        )
        self.assertEqual(
            compile(tb.last_name.istartswith('Sm')),
            ('"author"."last_name" ILIKE REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['Sm'])
        )
        self.assertEqual(
            compile(tb.last_name.contains('mi')),
            ('"author"."last_name" LIKE \'%%\' || REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.icontains('mi')),
            ('"author"."last_name" ILIKE \'%%\' || REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.endswith('th')),
            ('"author"."last_name" LIKE \'%%\' || REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') ESCAPE \'!\'', ['th'])
        )
        self.assertEqual(
            compile(tb.last_name.iendswith('th')),
            ('"author"."last_name" ILIKE \'%%\' || REPLACE(REPLACE(REPLACE(%s, \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') ESCAPE \'!\'', ['th'])
        )

        self.assertEqual(
            compile(tb.last_name.rstartswith('Sm')),
            ('%s LIKE REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['Sm'])
        )
        self.assertEqual(
            compile(tb.last_name.ristartswith('Sm')),
            ('%s ILIKE REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['Sm'])
        )
        self.assertEqual(
            compile(tb.last_name.rcontains('mi')),
            ('%s LIKE \'%%\' || REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.ricontains('mi')),
            ('%s ILIKE \'%%\' || REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') || \'%%\' ESCAPE \'!\'', ['mi'])
        )
        self.assertEqual(
            compile(tb.last_name.rendswith('th')),
            ('%s LIKE \'%%\' || REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') ESCAPE \'!\'', ['th'])
        )
        self.assertEqual(
            compile(tb.last_name.riendswith('th')),
            ('%s ILIKE \'%%\' || REPLACE(REPLACE(REPLACE("author"."last_name", \'!\', \'!!\'), \'_\', \'!_\'), \'%%\', \'!%%\') ESCAPE \'!\'', ['th'])
        )
        self.assertEqual(
            compile(+tb.counter),
            ('+"author"."counter"', [])
        )
        self.assertEqual(
            compile(-tb.counter),
            ('-"author"."counter"', [])
        )
        self.assertEqual(
            compile(~tb.counter),
            ('NOT "author"."counter"', [])
        )
        self.assertEqual(
            compile(tb.name.distinct()),
            ('DISTINCT "author"."name"', [])
        )
        self.assertEqual(
            compile(tb.counter ** 2),
            ('POW("author"."counter", %s)', [2])
        )
        self.assertEqual(
            compile(2 ** tb.counter),
            ('POW(%s, "author"."counter")', [2])
        )
        self.assertEqual(
            compile(tb.counter % 2),
            ('MOD("author"."counter", %s)', [2])
        )
        self.assertEqual(
            compile(2 % tb.counter),
            ('MOD(%s, "author"."counter")', [2])
        )
        self.assertEqual(
            compile(abs(tb.counter)),
            ('ABS("author"."counter")', [])
        )
        self.assertEqual(
            compile(tb.counter.count()),
            ('COUNT("author"."counter")', [])
        )
        self.assertEqual(
            compile(tb.age.between(20, 30)),
            ('"author"."age" BETWEEN %s AND %s', [20, 30])
        )
        self.assertEqual(
            compile(tb.age[20:30]),
            ('"author"."age" BETWEEN %s AND %s', [20, 30])
        )
        self.assertEqual(
            compile(T.tb.cl[T.tb.cl2:T.tb.cl3]),
            ('"tb"."cl" BETWEEN "tb"."cl2" AND "tb"."cl3"', [])
        )
        self.assertEqual(
            compile(tb.age[20]),
            ('"author"."age" = %s', [20])
        )
        self.assertEqual(
            compile(tb.name.concat(' staff', ' admin')),
            ('"author"."name" || %s || %s', [' staff', ' admin'])
        )
        self.assertEqual(
            compile(tb.name.concat_ws(' ', 'staff', 'admin')),
            ('concat_ws(%s, "author"."name", %s, %s)', [' ', 'staff', 'admin'])
        )
        self.assertEqual(
            compile(tb.name.op('MY_EXTRA_OPERATOR')(10)),
            ('"author"."name" MY_EXTRA_OPERATOR %s', [10])
        )
        self.assertEqual(
            compile(tb.name.rop('MY_EXTRA_OPERATOR')(10)),
            ('%s MY_EXTRA_OPERATOR "author"."name"', [10])
        )
        self.assertEqual(
            compile(tb.name.asc()),
            ('"author"."name" ASC', [])
        )
        self.assertEqual(
            compile(tb.name.desc()),
            ('"author"."name" DESC', [])
        )
        self.assertEqual(
            compile(((tb.age > 25) | (tb.answers > 10)) & (tb.is_staff | tb.is_admin)),
            ('("author"."age" > %s OR "author"."answers" > %s) AND ("author"."is_staff" OR "author"."is_admin")', [25, 10])
        )
        self.assertEqual(
            compile((T.author.first_name != 'Tom') & (T.author.last_name.in_(('Smith', 'Johnson')))),
            ('"author"."first_name" <> %s AND "author"."last_name" IN (%s, %s)', ['Tom', 'Smith', 'Johnson'])
        )
        self.assertEqual(
            compile((T.author.first_name != 'Tom') | (T.author.last_name.in_(('Smith', 'Johnson')))),
            ('"author"."first_name" <> %s OR "author"."last_name" IN (%s, %s)', ['Tom', 'Smith', 'Johnson'])
        )


class TestCaseExpr(TestCase):

    def test_case(self):
        self.assertEqual(
            compile(Case([
                (F.a == 1, 'one'),
                (F.b == 2, 'two'),
            ])),
            ('CASE WHEN ("a" = %s) THEN %s WHEN ("b" = %s) THEN %s END ', [1, 'one', 2, 'two'])
        )

    def test_case_with_default(self):
        self.assertEqual(
            compile(Case([
                (F.a == 1, 'one'),
                (F.b == 2, 'two'),
            ], default='other')),
            ('CASE WHEN ("a" = %s) THEN %s WHEN ("b" = %s) THEN %s ELSE %s END ', [1, 'one', 2, 'two', 'other'])
        )

    def test_case_with_expr(self):
        self.assertEqual(
            compile(Case([
                (1, 'one'),
                (2, 'two'),
            ], F.a)),
            ('CASE "a" WHEN %s THEN %s WHEN %s THEN %s END ', [1, 'one', 2, 'two'])
        )

    def test_case_with_expr_and_default(self):
        self.assertEqual(
            compile(Case([
                (1, 'one'),
                (2, 'two'),
            ], F.a, 'other')),
            ('CASE "a" WHEN %s THEN %s WHEN %s THEN %s ELSE %s END ', [1, 'one', 2, 'two', 'other'])
        )

    def test_case_in_query(self):
        self.assertEqual(
            compile(Q().tables(T.t1).fields('*').where(F.c == Case([
                (F.a == 1, 'one'),
                (F.b == 2, 'two'),
            ], default='other'))),
            ('SELECT * FROM "t1" WHERE "c" = CASE WHEN ("a" = %s) THEN %s WHEN ("b" = %s) THEN %s ELSE %s END ', [1, 'one', 2, 'two', 'other'])
        )


class TestCompositeExpr(unittest.TestCase):

    def test_compositeexpr(self):
        pk = CompositeExpr(T.tb.obj_id, T.tb.land_id, T.tb.date)
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk == (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id", "tb"."land_id", "tb"."date", "tb"."title" FROM "tb" WHERE "tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s', [1, 'en', today])
        )
        pk2 = pk.as_(('al1', 'al2', 'al3'))
        self.assertEqual(
            Q(T.tb).fields(pk2, T.tb.title).where(pk2 == (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE "al1" = %s AND "al2" = %s AND "al3" = %s', [1, 'en', today])
        )
        self.assertEqual(
            Q(T.tb).fields(pk2, T.tb.title).where(pk2.in_(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE "al1" = %s AND "al2" = %s AND "al3" = %s OR "al1" = %s AND "al2" = %s AND "al3" = %s', [1, 'en', today, 2, 'en', today])
        )

        self.assertEqual(
            Q(T.tb).fields(pk2, T.tb.title).where(pk2.not_in(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE NOT ("al1" = %s AND "al2" = %s AND "al3" = %s OR "al1" = %s AND "al2" = %s AND "al3" = %s)', [1, 'en', today, 2, 'en', today])
        )


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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
        )
        self.assertEqual(
            Q(T.stats).insert(OrderedDict((
                ('object_type', 'author'),
                ('object_id', 15),
                ('counter', 1),
            )), on_duplicate_key_update=OrderedDict((
                ('counter', T.stats.counter + func.VALUES(T.stats.counter)),
            ))),
            ('INSERT INTO "stats" ("object_type", "object_id", "counter") VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE "counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES %s, %s, %s ON DUPLICATE KEY UPDATE "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES (%s, %s, %s), (%s, %s, %s) ON DUPLICATE KEY UPDATE "stats"."counter" = "stats"."counter" + VALUES("stats"."counter")', ['author', 15, 1, 'author', 16, 1])
        )
        self.assertEqual(
            Q().fields(
                T.stats.object_type, T.stats.object_id, T.stats.counter
            ).tables(T.stats).insert(
                values=('author', 15, 1),
                ignore=True
            ),
            ('INSERT IGNORE INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") VALUES %s, %s, %s', ['author', 15, 1])
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
            ('INSERT INTO "stats" ("stats"."object_type", "stats"."object_id", "stats"."counter") SELECT "old_stats"."object_type", "old_stats"."object_id", "old_stats"."counter" FROM "old_stats" ON DUPLICATE KEY UPDATE "stats"."counter" = "stats"."counter" + "old_stats"."counter"', [])
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
