from __future__ import absolute_import
from functools import reduce
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.expressions import Expr, Operable, Value, datatypeof, func
from sqlbuilder.smartsql.operator_registry import operator_registry
from sqlbuilder.smartsql.pycompat import string_types
from sqlbuilder.smartsql.utils import Undef

__all__ = (
    'Binary', 'NamedBinary', 'NamedCompound', 'Add', 'Sub', 'Mul', 'Div', 'Gt', 'Lt', 'Ge', 'Le', 'And', 'Or',
    'Eq', 'Ne', 'Is', 'IsNot', 'In', 'NotIn', 'RShift', 'LShift', 'EscapeForLike', 'Like', 'ILike',
    'Ternary', 'NamedTernary', 'Between', 'NotBetween',
    'Prefix', 'NamedPrefix', 'Not', 'All', 'Distinct', 'Exists',
    'Unary', 'NamedUnary', 'Pos', 'Neg',
    'Postfix', 'NamedPostfix', 'OrderDirection', 'Asc', 'Desc'
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


class EscapeForLike(Expr):

    __slots__ = ('expr',)

    escape = "!"
    escape_map = tuple(  # Ordering is important!
        (i, "!{0}".format(i)) for i in ('!', '_', '%')
    )

    def __init__(self, expr):
        Operable.__init__(self)
        self.expr = expr


@compile.when(EscapeForLike)
def compile_escapeforlike(compile, expr, state):
    escaped = expr.expr
    for k, v in expr.escape_map:
        escaped = func.Replace(escaped, Value(k), Value(v))
    compile(escaped, state)


class Like(NamedBinary):
    __slots__ = ('escape',)
    sql = 'LIKE'

    def __init__(self, left, right, escape=Undef):
        """
        :type escape: str | Undef
        """
        Operable.__init__(self)
        self.left = left
        self.right = right
        if isinstance(right, EscapeForLike):
            self.escape = right.escape
        else:
            self.escape = escape


class ILike(Like):
    __slots__ = ()
    sql = 'ILIKE'


@compile.when(Like)
def compile_like(compile, expr, state):
    compile_binary(compile, expr, state)
    if expr.escape is not Undef:
        state.sql.append(' ESCAPE ')
        compile(Value(expr.escape) if isinstance(expr.escape, string_types) else expr.escape, state)


# Ternary

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


# Prefix

class Prefix(Expr):

    __slots__ = ('expr', )

    def __init__(self, prefix, expr):
        Expr.__init__(self, prefix)
        self.expr = expr


@compile.when(Prefix)
def compile_prefix(compile, expr, state):
    state.sql.append(expr.sql)
    state.sql.append(SPACE)
    compile(expr.expr, state)


class NamedPrefix(Prefix):
    __slots__ = ()

    def __init__(self, expr):
        Operable.__init__(self)
        self.expr = expr


class Not(NamedPrefix):
    __slots__ = ()
    sql = 'NOT'


class All(NamedPrefix):
    __slots__ = ()
    sql = 'ALL'


class Distinct(NamedPrefix):
    __slots__ = ()
    sql = 'DISTINCT'


class Exists(NamedPrefix):
    __slots__ = ()
    sql = 'EXISTS'


# Unary

class Unary(Prefix):
    __slots__ = ()


@compile.when(Unary)
def compile_unary(compile, expr, state):
    state.sql.append(expr.sql)
    compile(expr.expr, state)


class NamedUnary(Unary):
    __slots__ = ()

    def __init__(self, expr):
        self.expr = expr


class Pos(NamedUnary):
    __slots__ = ()
    sql = '+'


class Neg(NamedUnary):
    __slots__ = ()
    sql = '-'


# Postfix

class Postfix(Expr):
    __slots__ = ('expr', )

    def __init__(self, expr, postfix):
        Expr.__init__(self, postfix)
        self.expr = expr


@compile.when(Postfix)
def compile_postfix(compile, expr, state):
    compile(expr.expr, state)
    state.sql.append(SPACE)
    state.sql.append(expr.sql)


class NamedPostfix(Postfix):
    __slots__ = ()

    def __init__(self, expr):
        Operable.__init__(self)
        self.expr = expr


class OrderDirection(NamedPostfix):
    __slots__ = ()

    def __init__(self, expr):
        Operable.__init__(self)
        if isinstance(expr, OrderDirection):
            expr = expr.expr
        self.expr = expr


class Asc(OrderDirection):
    __slots__ = ()
    sql = 'ASC'


class Desc(OrderDirection):
    __slots__ = ()
    sql = 'DESC'
