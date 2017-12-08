import collections
from sqlbuilder import smartsql
from sqlbuilder.smartsql import pycompat
from sqlbuilder.smartsql.dialects import python
from sqlbuilder.smartsql.tests.base import TestCase


class Field(object):

    def __init__(self, name, column):
        self.name = name
        self.column = column

    def get_value(self, obj):
        return getattr(obj, self.name, None)

    def set_value(self, obj, value):
        return setattr(obj, self.name, value)


class Author(object):
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name


class Mapper(object):
    db_table = None
    sql_table = None
    model = Author
    fields = collections.OrderedDict()

    def __init__(self, model, db_table, fields):
        self.model = model
        self.db_table = db_table
        self.sql_table = smartsql.Table(db_table)
        self.fields = collections.OrderedDict()
        for field in fields:
            self.fields[field.name] = field

    @property
    def query(self):
        return smartsql.Query(
            self.sql_table
        ).fields(
            self.get_sql_fields()
        )

    def get_sql_fields(self, prefix=None):
        if prefix is None:
            prefix = self.sql_table
        elif isinstance(prefix, pycompat.string_types):
            prefix = smartsql.Table(prefix)
        return [prefix.get_field(f.name) for f in self.fields.values()]

    def get_sql_values(self, obj):
        row = {}
        for field in self.fields.values():
            key = python.execute.get_row_key(self.sql_table.get_field(field.column))
            row[key] = field.get_value(obj)
        return row


author_mapper = Mapper(Author, 'author', (
    Field('first_name', 'first_name'),
    Field('last_name', 'last_name')
))


class TestExecutor(TestCase):

    def test_mapper(self):
        obj = Author('Ivan', 'Zakrevsky')
        data = author_mapper.get_sql_values(obj)
        self.assertEqual(data['"author"."first_name"'], 'Ivan')
        self.assertEqual(data['"author"."last_name"'], 'Zakrevsky')

    def test_field(self):
        obj = Author('Ivan', 'Zakrevsky')
        state = python.State()
        state.row.update(author_mapper.get_sql_values(obj))
        first_name = python.execute(author_mapper.sql_table.f.first_name, state)
        self.assertEqual(first_name, 'Ivan')
        last_name = python.execute(author_mapper.sql_table.f.last_name, state)
        self.assertEqual(last_name, 'Zakrevsky')

    def test_add(self):
        self.assertEqual(python.execute(smartsql.Param(2) + 3, python.State()), 5)
        self.assertNotEqual(python.execute(smartsql.Param(2) + 3, python.State()), 6)

    def test_sub(self):
        self.assertEqual(python.execute(smartsql.Param(5) - 2, python.State()), 3)
        self.assertNotEqual(python.execute(smartsql.Param(5) - 2, python.State()), 4)

    def test_mul(self):
        self.assertEqual(python.execute(smartsql.Param(3) * 2, python.State()), 6)
        self.assertNotEqual(python.execute(smartsql.Param(3) * 2, python.State()), 7)

    def test_div(self):
        self.assertAlmostEqual(python.execute(smartsql.Param(5.0) / 2, python.State()), 2.5)
        self.assertNotAlmostEquals(python.execute(smartsql.Param(5.0) / 2, python.State()), 3)

    def test_gt(self):
        self.assertTrue(python.execute(smartsql.Param(3) > 2, python.State()))
        self.assertFalse(python.execute(smartsql.Param(2) > 3, python.State()))

    def test_ge(self):
        self.assertTrue(python.execute(smartsql.Param(3) >= 2, python.State()))
        self.assertTrue(python.execute(smartsql.Param(3) >= 3, python.State()))
        self.assertFalse(python.execute(smartsql.Param(2) >= 3, python.State()))

    def test_lt(self):
        self.assertTrue(python.execute(smartsql.Param(2) < 3, python.State()))
        self.assertFalse(python.execute(smartsql.Param(3) < 2, python.State()))

    def test_le(self):
        self.assertTrue(python.execute(smartsql.Param(2) <= 3, python.State()))
        self.assertTrue(python.execute(smartsql.Param(2) <= 3, python.State()))
        self.assertFalse(python.execute(smartsql.Param(3) <= 2, python.State()))

    def test_eq(self):
        self.assertTrue(python.execute(smartsql.Param(3) == 3, python.State()))
        self.assertFalse(python.execute(smartsql.Param(3) == 2, python.State()))

    def test_ne(self):
        self.assertTrue(python.execute(smartsql.Param(3) != 2, python.State()))
        self.assertFalse(python.execute(smartsql.Param(3) != 3, python.State()))

    def test_and(self):
        self.assertTrue(python.execute(smartsql.Param(True) & True, python.State()))
        self.assertFalse(python.execute(smartsql.Param(True) & False, python.State()))
        self.assertFalse(python.execute(smartsql.Param(False) & False, python.State()))

    def test_or(self):
        self.assertTrue(python.execute(smartsql.Param(True) | False, python.State()))
        self.assertTrue(python.execute(smartsql.Param(True) | True, python.State()))
        self.assertFalse(python.execute(smartsql.Param(False) | False, python.State()))
