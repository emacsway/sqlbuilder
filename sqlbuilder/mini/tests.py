from __future__ import absolute_import
import unittest
from sqlbuilder.mini import P, compile

__all__ = ('TestMini',)


class TestCase(unittest.TestCase):

    maxDiff = None


class TestMini(TestCase):

    def test_mini(self):

        sql = [
            'SELECT', [
                'author.id', 'author.first_name', 'author.last_name'
            ],
            'FROM', [
                'author', 'INNER JOIN', ['book as b', 'ON', 'b.author_id = author.id']
            ],
            'WHERE', [
                'b.status', '==', P('new')
            ],
            'ORDER BY', [
                'author.first_name', 'author.last_name'
            ]
        ]

        # Let change query
        sql[sql.index('SELECT') + 1].append('author.age')

        self.assertEqual(
            compile(sql),
            ('SELECT author.id, author.first_name, author.last_name, author.age FROM author INNER JOIN book as b ON b.author_id = author.id WHERE b.status == %s ORDER BY author.first_name, author.last_name', ['new'])
        )

    def test_mini_precompiled(self):

        sql = [
            'SELECT', [
                'author.id', 'author.first_name', 'author.last_name'
            ],
            'FROM', [
                'author', 'INNER JOIN', ['book as b', 'ON', 'b.author_id = author.id']
            ],
            'WHERE', [
                'b.status == %(status)s'
            ],
            'ORDER BY', [
                'author.first_name', 'author.last_name'
            ]
        ]

        # Let change query
        sql[sql.index('SELECT') + 1].append('author.age')

        sql_str = compile(sql)[0]
        self.assertEqual(
            (sql_str, {'status': 'new'}),
            ('SELECT author.id, author.first_name, author.last_name, author.age FROM author INNER JOIN book as b ON b.author_id = author.id WHERE b.status == %(status)s ORDER BY author.first_name, author.last_name', {'status': 'new'})
        )
