from __future__ import absolute_import
import re
import unittest
from sqlbuilder.mini import P, Q, compile

__all__ = ('TestMini', 'TestMiniQ')


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


class TestMiniQ(TestCase):

    def setUp(self):
        self._sql = [
            'SELECT', [
                'author.id', 'author.first_name', 'author.last_name'
            ],
            'FROM', [
                'author', 'INNER JOIN', [
                    '(', 'SELECT', [
                        'book.title'
                    ],
                    'FROM', [
                        'book'
                    ],
                    ')', 'AS b', 'ON', 'b.author_id = author.id'
                ],
            ],
            'WHERE', [
                'b.status', '==', P('new')
            ],
            'ORDER BY', [
                'author.first_name', 'author.last_name'
            ]
        ]

    def test_mini_q(self):

        sql = Q(self._sql)
        sql.prepend_child(
            ['FROM', 'INNER JOIN', 'SELECT'],
            ['book.id', 'book.pages']
        )
        sql.append_child(
            ['FROM', 'INNER JOIN', 'SELECT'],
            ['book.date']
        )
        sql.insert_after(
            ['FROM', 'INNER JOIN', (list, 1), ],
            ['WHERE', ['b.pages', '>', P(100)]]
        )
        sql.insert_before(
            ['FROM', 'INNER JOIN', 'WHERE', 'b.pages'],
            ['b.pages', '<', P(500), 'AND']
        )

        sql.append_child(
            ['FROM', 'INNER JOIN', (lambda i, item, collection: item == 'SELECT')],
            ['book.added_by_callable']
        )
        sql.append_child(
            ['FROM', 'INNER JOIN', ('SELECT', 0)],
            ['book.added_by_tuple']
        )
        sql.append_child(
            ['FROM', enumerate, 'SELECT'],
            ['book.added_by_each']
        )
        sql.append_child(
            ['FROM', 'INNER JOIN', 1],
            ['book.added_by_index']
        )
        sql.append_child(
            ['FROM', 'INNER JOIN', re.compile("^SELECT$")],
            ['book.added_by_re']
        )

        self.assertEqual(
            compile(sql),
            ('SELECT author.id, author.first_name, author.last_name FROM author INNER JOIN ( SELECT book.id, book.pages, book.title, book.date, book.added_by_callable, book.added_by_tuple, book.added_by_each, book.added_by_index, book.added_by_re FROM book WHERE b.pages < %s AND b.pages > %s ) AS b ON b.author_id = author.id WHERE b.status == %s ORDER BY author.first_name, author.last_name', [500, 100, 'new'])
        )
