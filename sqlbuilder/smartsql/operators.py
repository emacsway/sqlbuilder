from __future__ import absolute_import
from functools import reduce
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.expressions import Expr, Operable, datatypeof
from sqlbuilder.smartsql.operator_registry import operator_registry

__all__ = (
    'Binary', 'NamedBinary', 'NamedCompound', 'Add', 'Sub', 'Mul', 'Div', 'Gt', 'Lt', 'Ge', 'Le', 'And', 'Or',
    'Eq', 'Ne', 'Is', 'IsNot', 'In', 'NotIn', 'RShift', 'LShift',
    'Ternary', 'NamedTernary', 'Between', 'NotBetween',
)

SPACE = " "


class Binary(Expr):
    __slots__ = ('left', 'right')

    def __init__(self, left, op, right):
        op = op.upper()
        datatype = operator_registry.get(op, (datatypeof(left), datatypeof(right)))[0]
        Expr.__init__(self, op, datatype=datatype)
        self.left = left
        self.right = right


@compile.when(Binary)
def compile_binary(compile, expr, state):
    compile(expr.left, state)
    state.sql.append(SPACE)
    state.sql.append(expr.sql)
    state.sql.append(SPACE)
    compile(expr.right, state)


class NamedBinary(Binary):
    __slots__ = ()

    def __init__(self, left, right):
        # Don't use multi-arguments form like And(*args)
        # Use reduce(operator.and_, args) or reduce(And, args) instead. SRP.
        datatype = operator_registry.get(self.sql, (datatypeof(left), datatypeof(right)))[0]
        Operable.__init__(self, datatype)
        self.left = left
        self.right = right


class NamedCompound(NamedBinary):
    __slots__ = ()

    def __init__(self, *exprs):
        self.left = reduce(self.__class__, exprs[:-1])
        self.right = exprs[-1]
        datatype = operator_registry.get(self.sql, (datatypeof(self.left), datatypeof(self.right)))[0]
        Operable.__init__(self, datatype)


class Add(NamedCompound):
    sql = '+'


class Sub(NamedBinary):
    __slots__ = ()
    sql = '-'


class Mul(NamedCompound):
    __slots__ = ()
    sql = '*'


class Div(NamedBinary):
    __slots__ = ()
    sql = '/'


class Gt(NamedBinary):
    __slots__ = ()
    sql = '>'


class Lt(NamedBinary):
    __slots__ = ()
    sql = '<'


class Ge(NamedBinary):
    __slots__ = ()
    sql = '>='


class Le(NamedBinary):
    __slots__ = ()
    sql = '<='


class And(NamedCompound):
    __slots__ = ()
    sql = 'AND'


class Or(NamedCompound):
    __slots__ = ()
    sql = 'OR'


class Eq(NamedBinary):
    __slots__ = ()
    sql = '='


class Ne(NamedBinary):
    __slots__ = ()
    sql = '<>'


class Is(NamedBinary):
    __slots__ = ()
    sql = 'IS'


class IsNot(NamedBinary):
    __slots__ = ()
    sql = 'IS NOT'


class In(NamedBinary):
    __slots__ = ()
    sql = 'IN'


class NotIn(NamedBinary):
    __slots__ = ()
    sql = 'NOT IN'


class RShift(NamedBinary):
    __slots__ = ()
    sql = ">>"


class LShift(NamedBinary):
    __slots__ = ()
    sql = "<<"


class Ternary(Expr):
    __slots__ = ('second_sql', 'first', 'second', 'third')

    def __init__(self, first, sql, second, second_sql, third):
        Expr.__init__(self, sql)
        self.first = first
        self.second = second
        self.second_sql = second_sql
        self.third = third


@compile.when(Ternary)
def compile_ternary(compile, expr, state):
    compile(expr.first, state)
    state.sql.append(SPACE)
    state.sql.append(expr.sql)
    state.sql.append(SPACE)
    compile(expr.second, state)
    state.sql.append(SPACE)
    state.sql.append(expr.second_sql)
    state.sql.append(SPACE)
    compile(expr.third, state)


class NamedTernary(Ternary):
    __slots__ = ()

    def __init__(self, first, second, third):
        Operable.__init__(self)
        self.first = first
        self.second = second
        self.third = third


class Between(NamedTernary):
    __slots__ = ()
    sql = 'BETWEEN'
    second_sql = 'AND'


class NotBetween(Between):
    __slots__ = ()
    sql = 'NOT BETWEEN'
