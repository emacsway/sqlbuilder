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
        data = {}
        for field in self.fields.values():
            key = smartsql.compile(self.sql_table.get_field(field.column))[0]
            data[key] = field.get_value(obj)
        return data


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
        state.data.update(author_mapper.get_sql_values(obj))
        first_name = python.execute(author_mapper.sql_table.f.first_name, state)
        self.assertEqual(first_name, 'Ivan')
        last_name = python.execute(author_mapper.sql_table.f.last_name, state)
        self.assertEqual(last_name, 'Zakrevsky')
