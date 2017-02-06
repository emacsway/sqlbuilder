import datetime
from sqlbuilder.smartsql.tests.base import TestCase
from sqlbuilder.smartsql import Q, T, F, P, CompositeExpr, Case, Cast, compile

__all__ = ('TestExpr', 'TestCaseExpr', 'TestCallable', 'TestCompositeExpr', )


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
            ('POWER("author"."counter", %s)', [2])
        )
        self.assertEqual(
            compile(2 ** tb.counter),
            ('POWER(%s, "author"."counter")', [2])
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


class TestCallable(TestCase):

    def test_case(self):
        self.assertEqual(
            compile(Cast(F.field_name, 'text')),
            ('CAST("field_name" AS text)', [])
        )


class TestCompositeExpr(TestCase):

    def test_compositeexpr(self):
        pk = CompositeExpr(T.tb.obj_id, T.tb.land_id, T.tb.date)
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk == (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id", "tb"."land_id", "tb"."date", "tb"."title" FROM "tb" WHERE "tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s', [1, 'en', today])
        )
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk != (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id", "tb"."land_id", "tb"."date", "tb"."title" FROM "tb" WHERE "tb"."obj_id" <> %s AND "tb"."land_id" <> %s AND "tb"."date" <> %s', [1, 'en', today])
        )
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk.in_(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id", "tb"."land_id", "tb"."date", "tb"."title" FROM "tb" WHERE "tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s OR "tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s', [1, 'en', today, 2, 'en', today])
        )

        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk.not_in(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id", "tb"."land_id", "tb"."date", "tb"."title" FROM "tb" WHERE NOT ("tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s OR "tb"."obj_id" = %s AND "tb"."land_id" = %s AND "tb"."date" = %s)', [1, 'en', today, 2, 'en', today])
        )

    def test_compositeexpr_as_alias(self):
        pk = CompositeExpr(T.tb.obj_id, T.tb.land_id, T.tb.date).as_(('al1', 'al2', 'al3'))
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk == (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE "al1" = %s AND "al2" = %s AND "al3" = %s', [1, 'en', today])
        )
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk != (1, 'en', today)).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE "al1" <> %s AND "al2" <> %s AND "al3" <> %s', [1, 'en', today])
        )
        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk.in_(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE "al1" = %s AND "al2" = %s AND "al3" = %s OR "al1" = %s AND "al2" = %s AND "al3" = %s', [1, 'en', today, 2, 'en', today])
        )

        self.assertEqual(
            Q(T.tb).fields(pk, T.tb.title).where(pk.not_in(((1, 'en', today), (2, 'en', today)))).select(),
            ('SELECT "tb"."obj_id" AS "al1", "tb"."land_id" AS "al2", "tb"."date" AS "al3", "tb"."title" FROM "tb" WHERE NOT ("al1" = %s AND "al2" = %s AND "al3" = %s OR "al1" = %s AND "al2" = %s AND "al3" = %s)', [1, 'en', today, 2, 'en', today])
        )
