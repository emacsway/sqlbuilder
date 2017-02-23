from __future__ import absolute_import
import datetime
from sqlbuilder.smartsql.tests.base import TestCase

from sqlbuilder.smartsql import PLACEHOLDER, Q, T, F, A, E, Not, func, const, Result
from sqlbuilder.smartsql.dialects.mysql import compile as mysql_compile

__all__ = ('TestSmartSQLLegacy',)


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
            Q(T.tb).where(F.tb__cl__al == 5).select(F.tb__cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE "al" = %s', [5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl__al == 5).select(T.tb.cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE "al" = %s', [5, ], )
        )
        self.assertEqual(
            Q(T.tb).where(T.tb.cl.as_('al') == 5).select(T.tb.cl.as_('al')),
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
