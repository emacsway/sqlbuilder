# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/ , but implementation another.
# Pay attention also to excellent lightweight SQLBuilder
# of Storm ORM http://bazaar.launchpad.net/~storm/storm/trunk/view/head:/storm/expr.py
from __future__ import absolute_import

from sqlbuilder.smartsql.compiler import Compiler, State, cached_compile, compile
from sqlbuilder.smartsql.constants import CONTEXT, DEFAULT_DIALECT, LOOKUP_SEP, MAX_PRECEDENCE, OPERATORS, PLACEHOLDER
from sqlbuilder.smartsql.exceptions import Error, MaxLengthError, OperatorNotFound
from sqlbuilder.smartsql.expressions import (
    Operable, Expr, ExprList, CompositeExpr, Param, Parentheses, OmitParentheses,
    Callable, NamedCallable, Constant, ConstantSpace, Case, Cast,
    Alias, Name, NameCompiler, Value, ValueCompiler, Array, ArrayItem,
    expr_repr, datatypeof, const, func, compile_exprlist
)
from sqlbuilder.smartsql.factory import factory, Factory
from sqlbuilder.smartsql.fields import MetaFieldSpace, F, MetaField, Field, Subfield, FieldList
from sqlbuilder.smartsql.operator_registry import OperatorRegistry, operator_registry
from sqlbuilder.smartsql.operators import (
    Binary, NamedBinary, NamedCompound, Add, Sub, Mul, Div, Gt, Lt, Ge, Le, And, Or,
    Eq, Ne, Is, IsNot, In, NotIn, RShift, LShift, EscapeForLike, Like, ILike,
    Ternary, NamedTernary, Between, NotBetween,
    compile_binary
)
from sqlbuilder.smartsql.pycompat import str, string_types
from sqlbuilder.smartsql.tables import MetaTableSpace, T, MetaTable, FieldProxy, Table, TableAlias, TableJoin
from sqlbuilder.smartsql.utils import Undef, UndefType, is_allowed_attr, is_list, opt_checker, same, warn

SPACE = " "
Placeholder = Param
Condition = Binary
NamedCondition = NamedBinary


class Concat(ExprList):

    __slots__ = ('_ws', )

    def __init__(self, *args):
        super(Concat, self).__init__(*args)
        self.sql = ' || '
        self._ws = None

    def ws(self, sep=None):
        if sep is None:
            return self._ws
        self._ws = sep
        self.sql = ', '
        return self


@compile.when(Concat)
def compile_concat(compile, expr, state):
    if not expr.ws():
        return compile_exprlist(compile, expr, state)
    state.sql.append('concat_ws(')
    compile(expr.ws(), state)
    for a in expr:
        state.sql.append(expr.sql)
        compile(a, state)
    state.sql.append(')')


class Array(ExprList):  # TODO: use composition instead of inheritance, to solve ambiguous of __getitem__()???
    __slots__ = ()

    def __init__(self, *args):
        Operable.__init__(self)
        self.sql, self.data = ", ", list(args)


@compile.when(Array)
def compile_array(compile, expr, state):
    if not expr.data:
        state.sql.append("'{}'")
    state.sql.append("ARRAY[{0}]".format(compile_exprlist(compile, expr, state)))


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


def qn(name, compile):
    return compile(Name(name))[0]

from sqlbuilder.smartsql.queries import (
    Result, Executable, Select, Query, SelectCount, Raw,
    Modify, Insert, Update, Delete,
    Set, Union, Intersect, Except,
)

A, C, E, P, TA, Q, QS = Alias, Condition, Expr, Placeholder, TableAlias, Query, Query

compile.set_precedence(270, '.')
compile.set_precedence(260, '::')
compile.set_precedence(250, '[', ']')  # array element selection
compile.set_precedence(240, Pos, Neg, (Unary, '+'), (Unary, '-'), '~')  # unary minus
compile.set_precedence(230, '^')
compile.set_precedence(220, Mul, Div, (Binary, '*'), (Binary, '/'), (Binary, '%'))
compile.set_precedence(210, Add, Sub, (Binary, '+'), (Binary, '-'))
compile.set_precedence(200, LShift, RShift, '<<', '>>')
compile.set_precedence(190, '&')
compile.set_precedence(180, '#')
compile.set_precedence(170, '|')
compile.set_precedence(160, Is, 'IS')
compile.set_precedence(150, (Postfix, 'ISNULL'), (Postfix, 'NOTNULL'))
compile.set_precedence(140, '(any other)')  # all other native and user-defined operators
compile.set_precedence(130, In, NotIn, 'IN')
compile.set_precedence(120, Between, 'BETWEEN')
compile.set_precedence(110, 'OVERLAPS')
compile.set_precedence(100, Like, ILike, 'LIKE', 'ILIKE', 'SIMILAR')
compile.set_precedence(90, Lt, Gt, '<', '>')
compile.set_precedence(80, Le, Ge, Ne, '<=', '>=', '<>', '!=')
compile.set_precedence(70, Eq, '=')
compile.set_precedence(60, Not, 'NOT')
compile.set_precedence(50, And, 'AND')
compile.set_precedence(40, Or, 'OR')
compile.set_precedence(30, Set, Union, Intersect, Except)
compile.set_precedence(20, Select, Query, SelectCount, Raw, Insert, Update, Delete)
compile.set_precedence(10, Expr)
compile.set_precedence(None, All, Distinct)


from sqlbuilder.smartsql.datatypes import AbstractType, BaseType
operator_registry.register(OPERATORS.ADD, (BaseType, BaseType), BaseType, Add)
operator_registry.register(OPERATORS.SUB, (BaseType, BaseType), BaseType, Sub)
operator_registry.register(OPERATORS.MUL, (BaseType, BaseType), BaseType, Mul)
operator_registry.register(OPERATORS.DIV, (BaseType, BaseType), BaseType, Div)
operator_registry.register(OPERATORS.GT, (BaseType, BaseType), BaseType, Gt)
operator_registry.register(OPERATORS.LT, (BaseType, BaseType), BaseType, Lt)
operator_registry.register(OPERATORS.GE, (BaseType, BaseType), BaseType, Ge)
operator_registry.register(OPERATORS.LE, (BaseType, BaseType), BaseType, Le)
operator_registry.register(OPERATORS.AND, (BaseType, BaseType), BaseType, And)
operator_registry.register(OPERATORS.OR, (BaseType, BaseType), BaseType, Or)
operator_registry.register(OPERATORS.EQ, (BaseType, BaseType), BaseType, Eq)
operator_registry.register(OPERATORS.NE, (BaseType, BaseType), BaseType, Ne)
operator_registry.register(OPERATORS.IS, (BaseType, BaseType), BaseType, Is)
operator_registry.register(OPERATORS.IS_NOT, (BaseType, BaseType), BaseType, IsNot)
operator_registry.register(OPERATORS.IN, (BaseType, BaseType), BaseType, In)
operator_registry.register(OPERATORS.NOT_IN, (BaseType, BaseType), BaseType, NotIn)
operator_registry.register(OPERATORS.RSHIFT, (BaseType, BaseType), BaseType, RShift)
operator_registry.register(OPERATORS.LSHIFT, (BaseType, BaseType), BaseType, LShift)
operator_registry.register(OPERATORS.LIKE, (BaseType, BaseType), BaseType, Like)
operator_registry.register(OPERATORS.ILIKE, (BaseType, BaseType), BaseType, ILike)
