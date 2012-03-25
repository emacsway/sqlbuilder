import unittest

if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )))

from sqlbuilder.smartsql import QS, T, F, A, E, Prefix, func, const


class TestSmartSQL(unittest.TestCase):

    def test_prefix(self):
        self.assertEqual(
            QS(T.tb).where(~T.tb.cl == 3).select('*'),
            ('SELECT * FROM tb WHERE (NOT tb.cl = %s)', [3, ], )
        )
        self.assertEqual(
            QS(T.tb).where(Prefix((T.tb.cl == 2), (T.tb.cl2 == 3))).select('*'),
            ('SELECT * FROM tb WHERE tb.cl = %s (tb.cl2 = %s)', [3, ], )
        )

    def test_mod(self):
        self.assertEqual(
            QS(T.tb).where((T.tb.cl % 5) == 3).select('*'),
            ('SELECT * FROM tb WHERE (MOD(tb.cl, %s) = %s)', [5, 3, ], )
        )
        self.assertEqual(
            QS(T.tb).where((T.tb.cl % T.tb.cl2) == 3).select('*'),
            ('SELECT * FROM tb WHERE (MOD(tb.cl, tb.cl2) = %s)', [3, ], )
        )
        self.assertEqual(
            QS(T.tb).where((100 % T.tb.cl2) == 3).select('*'),
            ('SELECT * FROM tb WHERE (MOD(%s, tb.cl2) = %s)', [100, 3, ], )
        )

    def test_distinct(self):
        self.assertEqual(
            QS(T.tb).select('*'),
            ('SELECT * FROM tb', [], )
        )
        self.assertEqual(
            QS(T.tb).distinct(False).select('*'),
            ('SELECT * FROM tb', [], )
        )
        self.assertEqual(
            QS(T.tb).distinct(True).select('*'),
            ('SELECT DISTINCT * FROM tb', [], )
        )

    def test_function(self):
        self.assertEqual(
            QS(T.tb).where(func.FUNC_NAME(T.tb.cl) == 5).select('*'),
            ('SELECT * FROM tb WHERE (FUNC_NAME(tb.cl) = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl == func.RAND()).select('*'),
            ('SELECT * FROM tb WHERE (tb.cl = RAND())', [], )
        )

    def test_constant(self):
        self.assertEqual(
            QS(T.tb).where(const.CONST_NAME == 5).select('*'),
            ('SELECT * FROM tb WHERE (CONST_NAME = %s)', [5, ], )
        )

    def test_in(self):
        self.assertEqual(
            QS(T.tb).where(T.tb.cl == [1, T.tb.cl3, 5, ]).where(T.tb.cl2 == [1, T.tb.cl4, ]).select('*'),
            ('SELECT * FROM tb WHERE (tb.cl IN (%s, tb.cl3, %s) AND tb.cl2 IN (%s, tb.cl4))', [1, 5, 1, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl != [1, 3, 5, ]).select('*'),
            ('SELECT * FROM tb WHERE tb.cl NOT IN (%s, %s, %s)', [1, 3, 5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.in_([1, 3, 5, ])).select('*'),
            ('SELECT * FROM tb WHERE tb.cl IN (%s, %s, %s)', [1, 3, 5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl.not_in([1, 3, 5, ])).select('*'),
            ('SELECT * FROM tb WHERE tb.cl NOT IN (%s, %s, %s)', [1, 3, 5, ], )
        )

    def test_between(self):
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[5:15]).select('*'),
            ('SELECT * FROM tb WHERE tb.cl BETWEEN %s AND %s', [5, 15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[T.tb.cl2:15]).select('*'),
            ('SELECT * FROM tb WHERE tb.cl BETWEEN tb.cl2 AND %s', [15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[15:T.tb.cl3]).select('*'),
            ('SELECT * FROM tb WHERE tb.cl BETWEEN %s AND tb.cl3', [15, ], )
        )
        self.assertEqual(
            QS(T.tb).where(T.tb.cl[T.tb.cl2:T.tb.cl3]).select('*'),
            ('SELECT * FROM tb WHERE tb.cl BETWEEN tb.cl2 AND tb.cl3', [], )
        )

    def test_alias(self):
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(F.tb__cl__al),
            ('SELECT tb.cl AS al FROM tb WHERE (al = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(T.tb.cl__al),
            ('SELECT tb.cl AS al FROM tb WHERE (al = %s)', [5, ], )
        )
        self.assertEqual(
            QS(T.tb).where(A('al') == 5).select(T.tb.cl.as_('al')),
            ('SELECT tb.cl AS al FROM tb WHERE (al = %s)', [5, ], )
        )

if __name__ == '__main__':
    unittest.main()
