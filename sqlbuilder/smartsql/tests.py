from __future__ import absolute_import
import datetime
import unittest
from collections import OrderedDict

if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )))

from sqlbuilder.smartsql import PLACEHOLDER, QS, T, F, A, E, Prefix, Constant, func, const
from sqlbuilder.smartsql.compilers.mysql import compile as mysql_compile


class TestSmartSQL(unittest.TestCase):

    maxDiff = None

    def test_join(self):
        t1, t2, t3, t4 = T.t1, T.t2.as_('al2'), T.t3, T.t4
        self.assertEqual(
            QS((t1 + t2.on(t2.t1_id == t1.id)) * t3.on(t3.t2_id == t2.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            QS((t1 + t2).on(t2.t1_id == t1.id) * t3.on(t3.t2_id == t2.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            QS((t1 + ((t2 * t3).on(t3.t2_id == t2.id))()).on(t2.t1_id == t1.id) - t4.on(t4.t3_id == t3.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" LEFT OUTER JOIN ("t2" AS "al2" CROSS JOIN "t3" ON ("t3"."t2_id" = "al2"."id")) ON ("al2"."t1_id" = "t1"."id") RIGHT OUTER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")', [], )
        )
        self.assertEqual(
            QS(((t1 + t2) * t3 - t4)().on((t2.t1_id == t1.id) & (t3.t2_id == t2.id) & (t4.t3_id == t3.id))).select(t1.id),
            ('SELECT "t1"."id" FROM ("t1" LEFT OUTER JOIN "t2" AS "al2" CROSS JOIN "t3" RIGHT OUTER JOIN "t4") ON ((("al2"."t1_id" = "t1"."id") AND ("t3"."t2_id" = "al2"."id")) AND ("t4"."t3_id" = "t3"."id"))', [], )
        )
        self.assertEqual(
            QS((t1 & t2.on(t2.t1_id == t1.id) & (t3 & t4.on(t4.t3_id == t3.id))()).on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )
        self.assertEqual(
            QS(t1 & t2.on(t2.t1_id == t1.id) & (t3 & t4.on(t4.t3_id == t3.id)).as_nested().on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM "t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id") INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )
        self.assertEqual(
            QS((t1 & t2.on(t2.t1_id == t1.id))() & (t3 & t4.on(t4.t3_id == t3.id))().on(t3.t2_id == t2.id)).select(t1.id),
            ('SELECT "t1"."id" FROM ("t1" INNER JOIN "t2" AS "al2" ON ("al2"."t1_id" = "t1"."id")) INNER JOIN ("t3" INNER JOIN "t4" ON ("t4"."t3_id" = "t3"."id")) ON ("t3"."t2_id" = "al2"."id")', [], )
        )

    def test_hint(self):
        t1 = T.tb1
        t2 = T.tb1.as_('al2')
        q = QS(t1 & t2.hint(E('USE INDEX (`index1`, `index2`)')).on(t2.parent_id == t1.id)).set_compiler(mysql_compile)
        self.assertEqual(
            q.select(t2.id),
            ('SELECT `al2`.`id` FROM `tb1` INNER JOIN `tb1` AS `al2` ON (`al2`.`parent_id` = `tb1`.`id`) USE INDEX (`index1`, `index2`)', [], )
        )

    def test_prefix(self):
        self.assertEqual(
            QS(T.tb).where(~(T.tb.cl == 3)).select('*'),
            ('SELECT * FROM "tb" WHERE NOT ("tb"."cl" = %s)', [3, ], )
        )
        self.assertEqual(
            QS(T.tb).where(Prefix("NOT", (T.tb.cl == 3))).select('*'),
            ('SELECT * FROM "tb" WHERE NOT ("tb"."cl" = %s)', [3, ], )
        )

    def test_mod(self):
        self.assertEqual(
            QS(T.tb).where((T.tb.cl % 5) == 3).select('*'),
            ('SELECT * FROM "tb" WHERE (MOD("tb"."cl", %s) = %s)', [5, 3, ], )
        )
        self.assertEqual(
            QS(T.tb).where((T.tb.cl % T.tb.cl2) == 3).select('*'),
            ('SELECT * FROM "tb" WHERE (MOD("tb"."cl", "tb"."cl2") = %s)', [3, ], )
        )
        self.assertEqual(
            QS(T.tb).where((100 % T.tb.cl2) == 3).select('*'),
            ('SELECT * FROM "tb" WHERE (MOD(%s, "tb"."cl2") = %s)', [100, 3, ], )
        )

    def test_distinct(self):
        self.assertEqual(
            QS(T.tb).select('*'),
            ('SELECT * FROM "tb"', [], )
        )
        self.assertEqual(
            QS(T.tb).distinct(False).select('*'),
            ('SELECT * FROM "tb"', [], )
        )
        self.assertEqual(
            QS(T.tb).distinct(True).select('*'),
            ('SELECT DISTINCT * FROM "tb"', [], )
        )

    def test_function(self):
        self.assertEqual(
            QS(T.tb).where(func.FUNC_NAME(T.tb.cl) == 5).select('*'),
            ('SELECT * FROM "tb" WHERE (FUNC_NAME("tb"."cl") = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl == func.RAND()).select('*'),
            ('SELECT * FROM "tb" WHERE ("tb"."cl" = RAND())', [], )
        )

    def test_constant(self):
        self.assertEqual(
            QS(T.tb).where(const.CONST_NAME == 5).select('*'),
            ('SELECT * FROM "tb" WHERE (CONST_NAME = %s)', [5, ], )
        )

    def test_in(self):
        self.assertEqual(
            QS(T.tb).where(T.tb.cl == [1, T.tb.cl3, 5, ]).where(T.tb.cl2 == [1, T.tb.cl4, ]).select('*'),
            ('SELECT * FROM "tb" WHERE (("tb"."cl" IN (%s, "tb"."cl3", %s)) AND ("tb"."cl2" IN (%s, "tb"."cl4")))', [1, 5, 1, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl != [1, 3, 5, ]).select('*'),
            ('SELECT * FROM "tb" WHERE ("tb"."cl" NOT IN (%s, %s, %s))', [1, 3, 5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.in_([1, 3, 5, ])).select('*'),
            ('SELECT * FROM "tb" WHERE ("tb"."cl" IN (%s, %s, %s))', [1, 3, 5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.not_in([1, 3, 5, ])).select('*'),
            ('SELECT * FROM "tb" WHERE ("tb"."cl" NOT IN (%s, %s, %s))', [1, 3, 5, ], )
        )

    def test_between(self):
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[5:15]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" BETWEEN %s AND %s', [5, 15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[T.tb.cl2:15]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" BETWEEN "tb"."cl2" AND %s', [15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[15:T.tb.cl3]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" BETWEEN %s AND "tb"."cl3"', [15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[T.tb.cl2:T.tb.cl3]).select('*'),
            ('SELECT * FROM "tb" WHERE "tb"."cl" BETWEEN "tb"."cl2" AND "tb"."cl3"', [], )
        )

    def test_concat(self):
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.concat(1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM "tb" WHERE ("tb"."cl" || %s || %s || %s || "tb"."cl2" <> %s)', [1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.concat_ws(' + ', 1, 2, 'str', T.tb.cl2) != 'str2').select('*'),
            ('SELECT * FROM "tb" WHERE (concat_ws(%s, "tb"."cl", %s, %s, %s, "tb"."cl2") <> %s)', [' + ', 1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.concat(1, 2, 'str', T.tb.cl2) != 'str2').set_compiler(mysql_compile).select('*'),
            ('SELECT * FROM `tb` WHERE (CONCAT(`tb`.`cl`, %s, %s, %s, `tb`.`cl2`) <> %s)', [1, 2, 'str', 'str2'], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.concat_ws(' + ', 1, 2, 'str', T.tb.cl2) != 'str2').set_compiler(mysql_compile).select('*'),
            ('SELECT * FROM `tb` WHERE (CONCAT_WS(%s, `tb`.`cl`, %s, %s, %s, `tb`.`cl2`) <> %s)', [' + ', 1, 2, 'str', 'str2'], )
        )

    def test_alias(self):
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(F.tb__cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE ("al" = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(T.tb.cl__al),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE ("al" = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(T.tb.cl.as_('al')),
            ('SELECT "tb"."cl" AS "al" FROM "tb" WHERE ("al" = %s)', [5, ], )
        )

    def test_qs_as_alias(self):
        t1 = QS(T.tb).where(T.tb.cl == 5).fields(T.tb.cl).as_table('t1')
        self.assertEqual(
            QS(t1).where(t1.cl == 5).select(t1.cl),
            ('SELECT "t1"."cl" FROM (SELECT "tb"."cl" FROM "tb" WHERE ("tb"."cl" = %s)) AS "t1" WHERE ("t1"."cl" = %s)', [5, 5, ], )
        )

    def test_order_by(self):
        self.assertEqual(
            QS(T.tb).fields(T.tb.id).order_by(T.tb.id).select(),
            (u'SELECT "tb"."id" FROM "tb" ORDER BY "tb"."id" ASC', [])
        )
        self.assertEqual(
            QS(T.tb).fields(T.tb.id).order_by(T.tb.id.asc()).select(),
            (u'SELECT "tb"."id" FROM "tb" ORDER BY "tb"."id" ASC', [])
        )
        self.assertEqual(
            QS(T.tb).fields(T.tb.id).order_by(T.tb.id.desc()).select(),
            (u'SELECT "tb"."id" FROM "tb" ORDER BY "tb"."id" DESC', [])
        )
        self.assertEqual(
            QS(T.tb).fields(T.tb.id).order_by(T.tb.id, desc=True).select(),
            (u'SELECT "tb"."id" FROM "tb" ORDER BY "tb"."id" DESC', [])
        )
        self.assertEqual(
            QS(T.tb).fields(T.tb.id).order_by(T.tb.id.desc(), desc=True).select(),
            (u'SELECT "tb"."id" FROM "tb" ORDER BY "tb"."id" DESC', [])
        )

    def test_complex(self):
        self.assertEqual(
            QS((T.base + T.grade).on((T.base.type == T.grade.item_type) & (F.base__type == 1)) + T.lottery).on(
                F.base__type == F.lottery__item_type
            ).where(
                (F.name == "name") & (F.status == 0) | (F.name == None)
            ).group_by(T.base.type).having(E("count(*)") > 1).select(F.type, F.grade__grade, F.lottery__grade),
            ('SELECT "type", "grade"."grade", "lottery"."grade" FROM "base" LEFT OUTER JOIN "grade" ON (("base"."type" = "grade"."item_type") AND ("base"."type" = %s)) LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE ((("name" = %s) AND ("status" = %s)) OR ("name" IS NULL)) GROUP BY "base"."type" HAVING ((count(*)) > %s)', [1, 'name', 0, 1, ], )
        )
        t = T.grade
        self.assertEqual(
            QS(t).limit(0, 100).select(F.name),
            ('SELECT "name" FROM "grade" LIMIT %s', [100], )
        )
        t = (t & T.base).on(F.grade__item_type == F.base__type)
        self.assertEqual(
            QS(t).order_by(F.grade__name, F.base__name, desc=True).select(F.grade__name, F.base__img),
            ('SELECT "grade"."name", "base"."img" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") ORDER BY "grade"."name" DESC, "base"."name" DESC', [], )
        )
        t = (t + T.lottery).on(F.base__type == F.lottery__item_type)
        self.assertEqual(
            QS(t).group_by(F.grade__grade).having(F.grade__grade > 0).select(F.grade__name, F.base__img, F.lottery__price),
            ('SELECT "grade"."name", "base"."img", "lottery"."price" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") GROUP BY "grade"."grade" HAVING ("grade"."grade" > %s)', [0, ], )
        )
        w = (F.base__type == 1)
        self.assertEqual(
            QS(t).where(w).select(F.grade__name, for_update=True),
            ('SELECT "grade"."name" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE ("base"."type" = %s) FOR UPDATE', [1, ], )
        )
        w = w & (F.grade__status == [0, 1])
        now = datetime.datetime.now()
        w = w | (F.lottery__add_time > "2009-01-01") & (F.lottery__add_time <= now)
        self.assertEqual(
            QS(t).where(w).limit(1).select(F.grade__name, F.base__img, F.lottery__price),
            ('SELECT "grade"."name", "base"."img", "lottery"."price" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE ((("base"."type" = %s) AND ("grade"."status" IN (%s, %s))) OR (("lottery"."add_time" > %s) AND ("lottery"."add_time" <= %s))) LIMIT %s', [1, 0, 1, '2009-01-01', now, 1], )
        )
        w = w & (F.base__status != [1, 2])
        self.assertEqual(
            QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("CASE 1 WHEN 1")),
            ('SELECT "grade"."name", "base"."img", "lottery"."price", (CASE 1 WHEN 1) FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE (((("base"."type" = %s) AND ("grade"."status" IN (%s, %s))) OR (("lottery"."add_time" > %s) AND ("lottery"."add_time" <= %s))) AND ("base"."status" NOT IN (%s, %s)))', [1, 0, 1, '2009-01-01', now, 1, 2, ], )
        )
        self.assertEqual(
            QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("CASE 1 WHEN " + PLACEHOLDER, 'exp_value').as_("exp_result")),
            ('SELECT "grade"."name", "base"."img", "lottery"."price", (CASE 1 WHEN %s) AS "exp_result" FROM "grade" INNER JOIN "base" ON ("grade"."item_type" = "base"."type") LEFT OUTER JOIN "lottery" ON ("base"."type" = "lottery"."item_type") WHERE (((("base"."type" = %s) AND ("grade"."status" IN (%s, %s))) OR (("lottery"."add_time" > %s) AND ("lottery"."add_time" <= %s))) AND ("base"."status" NOT IN (%s, %s)))', ['exp_value', 1, 0, 1, '2009-01-01', now, 1, 2, ], )
        )
        qs = QS(T.user)
        self.assertEqual(
            qs.select(F.name),
            ('SELECT "name" FROM "user"', [], )
        )
        qs = qs.tables((qs.tables() & T.address).on(F.user__id == F.address__user_id))
        self.assertEqual(
            qs.select(F.user__name, F.address__street),
            ('SELECT "user"."name", "address"."street" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id")', [], )
        )
        qs = qs.where(F.id == 1)
        self.assertEqual(
            qs.select(F.name, F.id),
            ('SELECT "name", "id" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id") WHERE ("id" = %s)', [1, ], )
        )
        qs = qs.where((F.address__city_id == [111, 112]) | E("address.city_id IS NULL"))
        self.assertEqual(
            qs.select(F.user__name, F.address__street, func.COUNT(Constant("*")).as_("count")),
            ('SELECT "user"."name", "address"."street", COUNT(*) AS "count" FROM "user" INNER JOIN "address" ON ("user"."id" = "address"."user_id") WHERE (("id" = %s) AND (("address"."city_id" IN (%s, %s)) OR (address.city_id IS NULL)))', [1, 111, 112, ], )
        )

    def test_subquery(self):
        sub_q = QS(T.tb2).fields(T.tb2.id2).where(T.tb2.id == T.tb1.tb2_id).limit(1)
        self.assertEqual(
            QS(T.tb1).where(T.tb1.tb2_id == sub_q).select(T.tb1.id),
            ('SELECT "tb1"."id" FROM "tb1" WHERE ("tb1"."tb2_id" = (SELECT "tb2"."id2" FROM "tb2" WHERE ("tb2"."id" = "tb1"."tb2_id") LIMIT %s))', [1], )
        )
        self.assertEqual(
            QS(T.tb1).where(T.tb1.tb2_id.in_(sub_q)).select(T.tb1.id),
            ('SELECT "tb1"."id" FROM "tb1" WHERE ("tb1"."tb2_id" IN (SELECT "tb2"."id2" FROM "tb2" WHERE ("tb2"."id" = "tb1"."tb2_id") LIMIT %s))', [1], )
        )
        self.assertEqual(
            QS(T.tb1).select(sub_q.as_('sub_value')),
            ('SELECT (SELECT "tb2"."id2" FROM "tb2" WHERE ("tb2"."id" = "tb1"."tb2_id") LIMIT %s) AS "sub_value" FROM "tb1"', [1], )
        )

    def test_expression(self):
        self.assertEqual(
            QS(T.tb1).select(E('5 * 3 - 2*8').as_('sub_value')),
            ('SELECT (5 * 3 - 2*8) AS "sub_value" FROM "tb1"', [], )
        )
        self.assertEqual(
            QS(T.tb1).select(E('(5 - 2) * 8 + (6 - 3) * 8').as_('sub_value')),
            ('SELECT ((5 - 2) * 8 + (6 - 3) * 8) AS "sub_value" FROM "tb1"', [], )
        )

    def test_union(self):
        a = QS(T.item).where(T.item.status != -1).fields(T.item.type, T.item.name, T.item.img)
        b = QS(T.gift).where(T.gift.storage > 0).columns(T.gift.type, T.gift.name, T.gift.img)
        self.assertEqual(
            (a.set(True) | b).order_by("type", "name", desc=True).limit(100, 10).select(),
            ('(SELECT "item"."type", "item"."name", "item"."img" FROM "item" WHERE ("item"."status" <> %s)) UNION ALL (SELECT "gift"."type", "gift"."name", "gift"."img" FROM "gift" WHERE ("gift"."storage" > %s)) ORDER BY %s DESC, %s DESC LIMIT %s OFFSET %s', [-1, 0, 'type', 'name', 10, 100 ], )
        )

    def test_count(self):
        self.assertEqual(
            QS(T.tb1).fields(T.tb1.f1, T.tb1.f1).where(T.tb1.f1 > 10).order_by(T.tb1.f1).count(),
            ('SELECT COUNT(1) AS "count_value" FROM (SELECT "tb1"."f1", "tb1"."f1" FROM "tb1" WHERE ("tb1"."f1" > %s)) AS "count_list"', [10])
        )

    def test_insert(self):
        self.assertEqual(
            QS(T.user).insert(OrderedDict((
                ("status", 0),
                ("gender", "male"),
                ("name", "garfield")
            )), ignore=True),
            ('INSERT IGNORE INTO "user" ("status", "gender", "name") VALUES (%s, %s, %s)', [0, 'male', 'garfield', ], )
        )
        fl = ("name", "gender", "status", "age")
        vl = (("garfield", "male", 0, 1), ("superwoman", "female", 0, 10))
        self.assertEqual(
            QS(T.user).insert_many(fl, vl, on_duplicate_key_update={"age": E("age + VALUES(age)")}),
            ('INSERT INTO "user" ("name", "gender", "status", "age") VALUES (%s, %s, %s, %s), (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE "age" = (age + VALUES(age))', ['garfield', 'male', 0, 1, 'superwoman', 'female', 0, 10, ], )
        )

    def test_update(self):
        self.assertEqual(
            QS(T.user).where(F.id == 100).update(OrderedDict((("status", 1), ("name", "nobody"))), ignore=True),
            ('UPDATE IGNORE "user" SET "status" = %s, "name" = %s WHERE ("id" = %s)', [1, 'nobody', 100, ], )
        )

    def test_delete(self):
        self.assertEqual(
            QS(T.user).where(F.status == 1).delete(),
            ('DELETE FROM "user" WHERE ("status" = %s)', [1, ], )
        )

if __name__ == '__main__':
    unittest.main()
