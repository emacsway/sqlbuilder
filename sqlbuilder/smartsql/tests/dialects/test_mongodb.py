from sqlbuilder import smartsql
from sqlbuilder.smartsql.dialects import mongodb
from sqlbuilder.smartsql.tests.base import TestCase


class TestField(TestCase):

    def test_field(self):
        pass


class TestBinary(TestCase):

    def test_eq(self):
        self.assertDictEqual(
            mongodb.compile(smartsql.T.author.name == 'Ivan'),
            {'name': {'$eq': 'Ivan'}}
        )

    def test_ne(self):
        self.assertDictEqual(
            mongodb.compile(smartsql.T.author.name != 'Ivan'),
            {'name': {'$ne': 'Ivan'}}
        )

    def test_gt(self):
        self.assertDictEqual(
            mongodb.compile(smartsql.T.author.age > 30),
            {'age': {'$gt': 30}}
        )

    def test_lt(self):
        self.assertDictEqual(
            mongodb.compile(smartsql.T.author.age < 30),
            {'age': {'$lt': 30}}
        )

    def test_le(self):
        self.assertDictEqual(
            mongodb.compile(smartsql.T.author.age <= 30),
            {'age': {'$lte': 30}}
        )
