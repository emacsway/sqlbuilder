from sqlbuilder.smartsql.tests.base import TestCase
from sqlbuilder.smartsql.utils import AutoName

__all__ = ('TestAutoName', )


class TestAutoName(TestCase):

    def test_autoname(self):
        auto_name = AutoName()
        unique_names = set()
        length = 10
        for i in range(length):
            unique_names.add(next(auto_name))
        self.assertEqual(len(unique_names), length)
