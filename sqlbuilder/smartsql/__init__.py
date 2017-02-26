# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/ , but implementation another.
# Pay attention also to excellent lightweight SQLBuilder
# of Storm ORM http://bazaar.launchpad.net/~storm/storm/trunk/view/head:/storm/expr.py
from __future__ import absolute_import

from sqlbuilder.smartsql.compiler import Compiler, State, cached_compile, compile
from sqlbuilder.smartsql.constants import CONTEXT, DEFAULT_DIALECT, LOOKUP_SEP, MAX_PRECEDENCE, OPERATOR, PLACEHOLDER
from sqlbuilder.smartsql.exceptions import Error, MaxLengthError, OperatorNotFound
from sqlbuilder.smartsql.expressions import (
    Operable, Expr, ExprList, CompositeExpr, Param, Parentheses, OmitParentheses,
    Callable, NamedCallable, Constant, ConstantSpace, Case, Cast, Concat,
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
    Prefix, NamedPrefix, Not, All, Distinct, Exists,
    Unary, NamedUnary, Pos, Neg,
    Postfix, NamedPostfix, OrderDirection, Asc, Desc,
    compile_binary
)
from sqlbuilder.smartsql.pycompat import str, string_types
from sqlbuilder.smartsql.tables import (
    MetaTableSpace, T, MetaTable, FieldProxy, Table, TableAlias, TableJoin,
    Join, InnerJoin, LeftJoin, RightJoin, FullJoin, CrossJoin, ModelRegistry, model_registry
)
from sqlbuilder.smartsql.utils import Undef, UndefType, AutoName, auto_name, is_allowed_attr, is_list, opt_checker, same, warn

from sqlbuilder.smartsql.queries import (
    Result, Executable, Select, Query, SelectCount, Raw,
    Modify, Insert, Update, Delete,
    Set, Union, Intersect, Except,
)

SPACE = " "
Placeholder = Param
Condition = Binary
NamedCondition = NamedBinary


def qn(name, compile):
    return compile(Name(name))[0]

A, C, E, P, TA, Q, QS = Alias, Condition, Expr, Placeholder, TableAlias, Query, Query

# compile.set_precedence(270, '.')
# compile.set_precedence(260, '::')
# compile.set_precedence(250, '[', ']')  # array element selection
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
operator_registry.register(OPERATOR.ADD, (BaseType, BaseType), BaseType, Add)
operator_registry.register(OPERATOR.SUB, (BaseType, BaseType), BaseType, Sub)
operator_registry.register(OPERATOR.MUL, (BaseType, BaseType), BaseType, Mul)
operator_registry.register(OPERATOR.DIV, (BaseType, BaseType), BaseType, Div)
operator_registry.register(OPERATOR.GT, (BaseType, BaseType), BaseType, Gt)
operator_registry.register(OPERATOR.LT, (BaseType, BaseType), BaseType, Lt)
operator_registry.register(OPERATOR.GE, (BaseType, BaseType), BaseType, Ge)
operator_registry.register(OPERATOR.LE, (BaseType, BaseType), BaseType, Le)
operator_registry.register(OPERATOR.AND, (BaseType, BaseType), BaseType, And)
operator_registry.register(OPERATOR.OR, (BaseType, BaseType), BaseType, Or)
operator_registry.register(OPERATOR.EQ, (BaseType, BaseType), BaseType, Eq)
operator_registry.register(OPERATOR.NE, (BaseType, BaseType), BaseType, Ne)
operator_registry.register(OPERATOR.IS, (BaseType, BaseType), BaseType, Is)
operator_registry.register(OPERATOR.IS_NOT, (BaseType, BaseType), BaseType, IsNot)
operator_registry.register(OPERATOR.IN, (BaseType, BaseType), BaseType, In)
operator_registry.register(OPERATOR.NOT_IN, (BaseType, BaseType), BaseType, NotIn)
operator_registry.register(OPERATOR.RSHIFT, (BaseType, BaseType), BaseType, RShift)
operator_registry.register(OPERATOR.LSHIFT, (BaseType, BaseType), BaseType, LShift)
operator_registry.register(OPERATOR.LIKE, (BaseType, BaseType), BaseType, Like)
operator_registry.register(OPERATOR.ILIKE, (BaseType, BaseType), BaseType, ILike)
