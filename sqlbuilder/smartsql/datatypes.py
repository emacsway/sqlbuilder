import sys
from sqlbuilder.smartsql.constants import OPERATOR
from sqlbuilder.smartsql.expressions import Alias, Concat, Value, datatypeof, func
from sqlbuilder.smartsql.operator_registry import operator_registry
from sqlbuilder.smartsql.operators import (
    Binary, EscapeForLike, Like, ILike, All, Asc, Desc, Between, Distinct, Neg, Not, Pos
)
from sqlbuilder.smartsql.utils import Undef, is_list, warn

__all__ = ('AbstractType', 'BaseType', )


# TODO: Datatype should be aware about its scheme/operator_registry. Pass operator_registry to constructor?
class AbstractType(object):
    __slots__ = ('_expr',)

    def __init__(self, expr):
        self._expr = expr  # weakref.ref(expr)

    def _op(self, operator, operands, *args, **kwargs):
        expression_factory = operator_registry.get(operator, tuple(map(datatypeof, operands)))[1]
        return expression_factory(*(operands + args), **kwargs)


class BaseType(AbstractType):

    def __add__(self, other):
        return self._op(OPERATOR.ADD, (self._expr, other))

    def __radd__(self, other):
        return self._op(OPERATOR.ADD, (other, self._expr))

    def __sub__(self, other):
        return self._op(OPERATOR.SUB, (self._expr, other))

    def __rsub__(self, other):
        return self._op(OPERATOR.SUB, (other, self._expr))

    def __mul__(self, other):
        return self._op(OPERATOR.MUL, (self._expr, other))

    def __rmul__(self, other):
        return self._op(OPERATOR.MUL, (other, self._expr))

    def __div__(self, other):
        return self._op(OPERATOR.DIV, (self._expr, other))

    def __rdiv__(self, other):
        return self._op(OPERATOR.DIV, (other, self._expr))

    def __truediv__(self, other):
        return self._op(OPERATOR.DIV, (self._expr, other))

    def __rtruediv__(self, other):
        return self._op(OPERATOR.DIV, (other, self._expr))

    def __floordiv__(self, other):
        return self._op(OPERATOR.DIV, (self._expr, other))

    def __rfloordiv__(self, other):
        return self._op(OPERATOR.DIV, (other, self._expr))

    def __and__(self, other):
        return self._op(OPERATOR.AND, (self._expr, other))

    def __rand__(self, other):
        return self._op(OPERATOR.AND, (other, self._expr))

    def __or__(self, other):
        return self._op(OPERATOR.OR, (self._expr, other))

    def __ror__(self, other):
        return self._op(OPERATOR.OR, (other, self._expr))

    def __gt__(self, other):
        return self._op(OPERATOR.GT, (self._expr, other))

    def __lt__(self, other):
        return self._op(OPERATOR.LT, (self._expr, other))

    def __ge__(self, other):
        return self._op(OPERATOR.GE, (self._expr, other))

    def __le__(self, other):
        return self._op(OPERATOR.LE, (self._expr, other))

    def __eq__(self, other):
        if other is None:
            return self.is_(None)
        if is_list(other):
            return self.in_(other)
        return self._op(OPERATOR.EQ, (self._expr, other))

    def __ne__(self, other):
        if other is None:
            return self.is_not(None)
        if is_list(other):
            return self.not_in(other)
        return self._op(OPERATOR.NE, (self._expr, other))

    def __rshift__(self, other):
        return self._op(OPERATOR.RSHIFT, (self._expr, other))

    def __rrshift__(self, other):
        return self._op(OPERATOR.RSHIFT, (other, self._expr))

    def __lshift__(self, other):
        return self._op(OPERATOR.LSHIFT, (self._expr, other))

    def __rlshift__(self, other):
        return self._op(OPERATOR.LSHIFT, (other, self._expr))

    def is_(self, other):
        return self._op(OPERATOR.IS, (self._expr, other))

    def is_not(self, other):
        return self._op(OPERATOR.IS_NOT, (self._expr, other))

    def in_(self, other):
        return self._op(OPERATOR.IN, (self._expr, other))

    def not_in(self, other):
        return self._op(OPERATOR.NOT_IN, (self._expr, other))

    def like(self, other, escape=Undef):
        return self._op(OPERATOR.LIKE, (self._expr, other), escape=escape)

    def ilike(self, other, escape=Undef):
        return ILike(self._expr, other, escape=escape)

    def rlike(self, other, escape=Undef):
        return Like(other, self._expr, escape=escape)

    def rilike(self, other, escape=Undef):
        return ILike(other, self._expr, escape=escape)

    def startswith(self, other):
        pattern = EscapeForLike(other)
        return Like(self._expr, Concat(pattern, Value('%')), escape=pattern.escape)

    def istartswith(self, other):
        pattern = EscapeForLike(other)
        return ILike(self._expr, Concat(pattern, Value('%')), escape=pattern.escape)

    def contains(self, other):  # TODO: ambiguous with "@>" operator of postgresql.
        pattern = EscapeForLike(other)
        return Like(self._expr, Concat(Value('%'), pattern, Value('%')), escape=pattern.escape)

    def icontains(self, other):
        pattern = EscapeForLike(other)
        return ILike(self._expr, Concat(Value('%'), pattern, Value('%')), escape=pattern.escape)

    def endswith(self, other):
        pattern = EscapeForLike(other)
        return Like(self._expr, Concat(Value('%'), pattern), escape=pattern.escape)

    def iendswith(self, other):
        pattern = EscapeForLike(other)
        return ILike(self._expr, Concat(Value('%'), pattern), escape=pattern.escape)

    def rstartswith(self, other):
        pattern = EscapeForLike(self._expr)
        return Like(other, Concat(pattern, Value('%')), escape=pattern.escape)

    def ristartswith(self, other):
        pattern = EscapeForLike(self._expr)
        return ILike(other, Concat(pattern, Value('%')), escape=pattern.escape)

    def rcontains(self, other):
        pattern = EscapeForLike(self._expr)
        return Like(other, Concat(Value('%'), pattern, Value('%')), escape=pattern.escape)

    def ricontains(self, other):
        pattern = EscapeForLike(self._expr)
        return ILike(other, Concat(Value('%'), pattern, Value('%')), escape=pattern.escape)

    def rendswith(self, other):
        pattern = EscapeForLike(self._expr)
        return Like(other, Concat(Value('%'), pattern), escape=pattern.escape)

    def riendswith(self, other):
        pattern = EscapeForLike(self._expr)
        return ILike(other, Concat(Value('%'), pattern), escape=pattern.escape)

    def __pos__(self):
        return Pos(self._expr)

    def __neg__(self):
        return Neg(self._expr)

    def __invert__(self):
        return Not(self._expr)

    def all(self):
        return All(self._expr)

    def distinct(self):
        return Distinct(self._expr)

    def __pow__(self, other):
        return func.Power(self._expr, other)

    def __rpow__(self, other):
        return func.Power(other, self._expr)

    def __mod__(self, other):
        return func.Mod(self._expr, other)

    def __rmod__(self, other):
        return func.Mod(other, self._expr)

    def __abs__(self):
        return func.Abs(self._expr)

    def count(self):
        return func.Count(self._expr)

    def as_(self, alias):
        return Alias(self._expr, alias)

    def between(self, start, end):
        return Between(self._expr, start, end)

    def concat(self, *args):
        return Concat(self._expr, *args)

    def concat_ws(self, sep, *args):
        return Concat(self._expr, *args).ws(sep)

    def op(self, op):
        return lambda other: Binary(self._expr, op, other)

    def rop(self, op):  # useless, can be P('lookingfor').op('=')(expr)
        return lambda other: Binary(other, op, self._expr)

    def asc(self):
        return Asc(self._expr)

    def desc(self):
        return Desc(self._expr)

    def __getitem__(self, key):
        """Returns self.between()"""
        # Is it should return ArrayItem(key) or Subfield(self._expr, key)?
        # Ambiguity with Query and ExprList!!!
        # Name conflict with Query.__getitem__(). Query can returns a single array.
        # We also may want to apply Between() or Eq() to subquery.
        if isinstance(key, slice):
            warn('__getitem__(slice(...))', 'between(start, end)')
            start = key.start or 0
            end = key.stop or sys.maxsize
            return Between(self._expr, start, end)
        else:
            warn('__getitem__(key)', '__eq__(key)')
            return self.__eq__(key)

    __hash__ = object.__hash__
