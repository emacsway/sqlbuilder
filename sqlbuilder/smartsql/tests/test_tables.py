from sqlbuilder.smartsql.tests.base import TestCase
from sqlbuilder.smartsql import (
    Q, T, Table, TableJoin, TA, E, Field,
    Join, InnerJoin, LeftJoin, RightJoin, FullJoin, CrossJoin,
    model_registry, compile
)
from sqlbuilder.smartsql.dialects.mysql import compile as mysql_compile

__all__ = ('TestTable', 'TestModelBasedTable', 'TestFieldProxy', )


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
            compile(T.author.get_field(('first_name', 'last_name')) == ('fn1', 'ln2')),
            ('"author"."first_name" = %s AND "author"."last_name" = %s', ['fn1', 'ln2'])
        )
        self.assertEqual(
            compile(T.author.get_field(('first_name__a', 'last_name__b')) == ('fn1', 'ln2')),
            ('"a" = %s AND "b" = %s', ['fn1', 'ln2'])
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

    def test_issue_20(self):
        t1, t2 = T.tb1, T.tb2
        tj = t2.on(t1.id == t2.id)
        self.assertEqual(
            compile(tj),
            ('"tb2" ON ("tb1"."id" = "tb2"."id")', [])
        )
        self.assertEqual(
            compile(t1 + tj),
            ('"tb1" LEFT OUTER JOIN "tb2" ON ("tb1"."id" = "tb2"."id")', [])
        )
        self.assertEqual(
            compile(t1 + tj),
            ('"tb1" LEFT OUTER JOIN "tb2" ON ("tb1"."id" = "tb2"."id")', [])
        )


class PropertyDescriptor(object):
    _field = None
    _value = None

    def _get_name(self, owner):
        for k, v in owner.__dict__.items():
            if v is self:
                return k

    def __get__(self, instance, owner):
        if instance is None:
            if self._field is None:
                self._field = Field(self._get_name(owner), owner)
            return self._field
        else:
            return self._value

    def __set__(self, instance, value):
        self._value = value


@model_registry.register('author')
class Author(object):
    id = PropertyDescriptor()
    first_name = PropertyDescriptor()
    last_name = PropertyDescriptor()


@model_registry.register('post')
class Post(object):
    id = PropertyDescriptor()
    title = PropertyDescriptor()
    text = PropertyDescriptor()
    author_id = PropertyDescriptor()


class TestModelBasedTable(TestCase):

    def test_model(self):
        self.assertIsInstance(Author.first_name, Field)
        self.assertEqual(
            compile(Author),
            ('"author"', [])
        )
        self.assertEqual(
            compile(Author.first_name),
            ('"author"."first_name"', [])
        )
        self.assertEqual(
            compile((TableJoin(Author) & Post).on(Post.author_id == Author.id)),
            ('"author" INNER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(Join(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(InnerJoin(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" INNER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(LeftJoin(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" LEFT OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(RightJoin(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" RIGHT OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(FullJoin(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" FULL OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(CrossJoin(Author, Post, on=(Post.author_id == Author.id))),
            ('"author" CROSS JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )


class TestFieldProxy(TestCase):

    def test_model(self):
        author = T.author.f
        post = T.post.f

        self.assertIsInstance(author.first_name, Field)
        self.assertEqual(
            compile(author),
            ('"author"', [])
        )
        self.assertEqual(
            compile(author.first_name),
            ('"author"."first_name"', [])
        )
        self.assertEqual(
            compile((TableJoin(author) & post).on(post.author_id == author.id)),
            ('"author" INNER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(Join(author, post, on=(post.author_id == author.id))),
            ('"author" JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(InnerJoin(author, post, on=(post.author_id == author.id))),
            ('"author" INNER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(LeftJoin(author, post, on=(post.author_id == author.id))),
            ('"author" LEFT OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(RightJoin(author, post, on=(post.author_id == author.id))),
            ('"author" RIGHT OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(FullJoin(author, post, on=(post.author_id == author.id))),
            ('"author" FULL OUTER JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
        self.assertEqual(
            compile(CrossJoin(author, post, on=(post.author_id == author.id))),
            ('"author" CROSS JOIN "post" ON ("post"."author_id" = "author"."id")', [])
        )
