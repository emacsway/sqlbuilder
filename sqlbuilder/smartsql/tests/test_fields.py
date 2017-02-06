from sqlbuilder.smartsql.tests.base import TestCase
from sqlbuilder.smartsql import Q, T, F, Field, A, compile

__all__ = ('TestField', )


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
