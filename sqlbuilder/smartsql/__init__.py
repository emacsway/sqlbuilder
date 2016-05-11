# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/ , but implementation another.
from __future__ import absolute_import
import sys
import copy
import types
import operator
import warnings
import weakref
from functools import wraps, reduce, partial

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

except NameError:
    string_types = (str,)
    integer_types = (int,)


DEFAULT_DIALECT = 'postgres'
PLACEHOLDER = "%s"  # Can be re-defined by registered dialect.
LOOKUP_SEP = '__'
MAX_PRECEDENCE = 1000
SPACE = " "

CONTEXT_QUERY = 0
CONTEXT_COLUMN = 1
CONTEXT_TABLE = 2


def same(name):
    def f(self, *a, **kw):
        return getattr(self, name)(*a, **kw)
    return f


class State(object):

    def __init__(self):
        self.sql = []
        self.params = []
        self._stack = []
        self.callers = []
        self.auto_tables = []
        self.join_tables = []
        self.context = CONTEXT_QUERY
        self.precedence = 0

    def push(self, attr, new_value=None):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is None:
            new_value = copy.copy(old_value)
        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        setattr(self, *self._stack.pop(-1))


class Compiler(object):

    def __init__(self, parent=None):
        self._children = weakref.WeakKeyDictionary()
        self._parents = []
        self._local_registry = {}
        self._local_precedence = {}
        self._registry = {}
        self._precedence = {}
        if parent:
            self._parents.extend(parent._parents)
            self._parents.append(parent)
            parent._children[self] = True
            self._update_cache()

    def create_child(self):
        return self.__class__(self)

    def when(self, cls):
        def deco(func):
            self._local_registry[cls] = func
            self._update_cache()
            return func
        return deco

    def set_precedence(self, precedence, *types):
        for type in types:
            self._local_precedence[type] = precedence
        self._update_cache()

    def _update_cache(self):
        for parent in self._parents:
            self._registry.update(parent._local_registry)
            self._precedence.update(parent._local_precedence)
        self._registry.update(self._local_registry)
        self._precedence.update(self._local_precedence)
        for child in self._children:
            child._update_cache()

    def __call__(self, expr, state=None):
        if state is None:
            state = State()
            self(expr, state)
            return ''.join(state.sql), state.params

        cls = expr.__class__
        parentheses = None
        outer_precedence = state.precedence
        inner_precedence = self.get_inner_precedence(expr)
        if inner_precedence is None:
            # pass current precedence
            # FieldList, ExprList, All, Distinct...?
            inner_precedence = outer_precedence
        state.precedence = inner_precedence
        if inner_precedence < outer_precedence:
            parentheses = True

        state.callers.insert(0, expr.__class__)

        if parentheses:
            state.sql.append('(')

        for c in cls.mro():
            if c in self._registry:
                self._registry[c](self, expr, state)
                break
        else:
            raise Error("Unknown compiler for {0}".format(cls))

        if parentheses:
            state.sql.append(')')
        state.callers.pop(0)
        state.precedence = outer_precedence

    def get_inner_precedence(self, expr):
        cls = expr.__class__
        if issubclass(cls, Expr) and hasattr(expr, '_sql'):
            try:
                if (cls, expr._sql) in self._precedence:
                    return self._precedence[(cls, expr._sql)]
                elif expr._sql in self._precedence:
                    return self._precedence[expr._sql]
            except TypeError:
                # For case when expr._sql is unhashable, for example we can allow T('tablename')._sql in future.
                # I'm not sure, whether Field() should be unhashable.
                pass

        if cls in self._precedence:
            return self._precedence[cls]
        return MAX_PRECEDENCE  # self._precedence.get('(any other)', MAX_PRECEDENCE)

compile = Compiler()


def opt_checker(k_list):
    def new_deco(f):
        @wraps(f)
        def new_func(self, *args, **opt):
            for k, v in list(opt.items()):
                if k not in k_list:
                    raise TypeError("Not implemented option: {0}".format(k))
            return f(self, *args, **opt)
        return new_func
    return new_deco


def cached_compile(f):
    @wraps(f)
    def deco(compile, expr, state):
        if compile not in expr.__cached__:
            state.push('sql', [])
            f(compile, expr, state)
            # TODO: also cache state.tables?
            expr.__cached__[compile] = ''.join(state.sql)
            state.pop()
        state.sql.append(expr.__cached__[compile])
    return deco


@compile.when(object)
def compile_object(compile, expr, state):
    state.sql.append(PLACEHOLDER)
    state.params.append(expr)


@compile.when(type(None))
def compile_none(compile, expr, state):
    state.sql.append('NULL')


@compile.when(list)
@compile.when(tuple)
def compile_list(compile, expr, state):
    compile(Parentheses(ExprList(*expr).join(", ")), state)


@compile.when(slice)
def compile_slice(compile, expr, state):
    # FIXME: Should be here numrange()? Looks like not, see http://initd.org/psycopg/docs/extras.html#adapt-range
    state.sql.append("[")
    state.sql.append("{0:d}".format(expr.start))
    if expr.stop is not None:
        state.sql.append(", ")
        state.sql.append("{0:d}".format(expr.stop))
    state.sql.append("]")


class Error(Exception):
    pass


class UndefType(object):

    def __repr__(self):
        return "Undef"

    def __reduce__(self):
        return "Undef"

Undef = UndefType()


class Comparable(object):

    __slots__ = ()

    def _ca(op, inv=False):
        return (lambda self, *a: Constant(op)(self, *a)) if not inv else (lambda self, other: Constant(op)(other, self))

    def _l(mask, ci=False, inv=False):
        def f(self, other):

            if ci:
                cls = Ilike
            else:
                cls = Like

            if inv:
                left, right = other, self
            else:
                left, right = self, other

            right = EscapeForLike(right)

            args = [right]
            if 4 & mask:
                args.insert(0, Value('%'))
            if 1 & mask:
                args.append(Value('%'))
            return cls(left, Concat(*args), escape=right._escape)  # other can be expression, so, using Concat()
        return f

    def __add__(self, other):
        return Add(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __sub__(self, other):
        return Sub(self, other)

    def __rsub__(self, other):
        return Sub(other, self)

    def __mul__(self, other):
        return Mul(self, other)

    def __rmul__(self, other):
        return Mul(other, self)

    def __div__(self, other):
        return Div(self, other)

    def __rdiv__(self, other):
        return Div(other, self)

    def __truediv__(self, other):
        return Div(self, other)

    def __rtruediv__(self, other):
        return Div(other, self)

    def __floordiv__(self, other):
        return Div(self, other)

    def __rfloordiv__(self, other):
        return Div(other, self)

    def __and__(self, other):
        return And(self, other)

    def __rand__(self, other):
        return And(other, self)

    def __or__(self, other):
        return Or(self, other)

    def __ror__(self, other):
        return Or(other, self)

    def __gt__(self, other):
        return Gt(self, other)

    def __lt__(self, other):
        return Lt(self, other)

    def __ge__(self, other):
        return Ge(self, other)

    def __le__(self, other):
        return Le(self, other)

    def __eq__(self, other):
        if other is None:
            return self.is_(None)
        if is_list(other):
            return self.in_(other)
        return Eq(self, other)

    def __ne__(self, other):
        if other is None:
            return self.is_not(None)
        if is_list(other):
            return self.not_in(other)
        return Ne(self, other)

    def __rshift__(self, other):
        return RShift(self, other)

    def __lshift__(self, other):
        return LShift(self, other)

    def is_(self, other):
        return Is(self, other)

    def is_not(self, other):
        return IsNot(self, other)

    def in_(self, other):
        return In(self, other)

    def not_in(self, other):
        return NotIn(self, other)

    def like(self, other, escape=Undef):
        return Like(self, other, escape)

    def ilike(self, other, escape=Undef):
        return Ilike(self, other, escape)

    def rlike(self, other, escape=Undef):
        return Like(other, self, escape)

    def rilike(self, other, escape=Undef):
        return Ilike(other, self, escape)

    startswith = _l(1)
    istartswith = _l(1, 1)
    contains = _l(5)
    icontains = _l(5, 1)
    endswith = _l(4)
    iendswith = _l(4, 1)
    rstartswith = _l(1, 0, 1)
    ristartswith = _l(1, 1, 1)
    rcontains = _l(5, 0, 1)
    ricontains = _l(5, 1, 1)
    rendswith = _l(4, 0, 1)
    riendswith = _l(4, 1, 1)

    def __pos__(self):
        return Pos(self)

    def __neg__(self):
        return Neg(self)

    def __invert__(self):
        return Not(self)

    def all(self):
        return All(self)

    def distinct(self):
        return Distinct(self)

    __pow__ = _ca("POW")
    __rpow__ = _ca("POW", 1)
    __mod__ = _ca("MOD")
    __rmod__ = _ca("MOD", 1)
    __abs__ = _ca("ABS")
    count = _ca("COUNT")

    def as_(self, alias):
        return Alias(alias, self)

    def between(self, start, end):
        return Between(self, start, end)

    def concat(self, *args):
        return Concat(self, *args)

    def concat_ws(self, sep, *args):
        return Concat(self, *args).ws(sep)

    def op(self, op):
        return lambda other: Binary(self, op, other)

    def rop(self, op):  # useless, can be P('lookingfor').op('=')(expr)
        return lambda other: Binary(other, op, self)

    def asc(self):
        return Asc(self)

    def desc(self):
        return Desc(self)

    def __getitem__(self, key):
        """Returns self.between()"""
        # Is it should return ArrayItem(key) or Subfield(self, key)?
        # Ambiguity with Query and ExprList!!!
        # Name conflict with Query.__getitem__(). Query can returns a single array.
        # We also may want to apply Between() or Eq() to subquery.
        if isinstance(key, slice):
            warn('__getitem__(slice(...))', 'between(start, end)')
            start = key.start or 0
            end = key.stop or sys.maxsize
            return Between(self, start, end)
        else:
            warn('__getitem__(key)', '__eq__(key)')
            return self.__eq__(key)

    __hash__ = object.__hash__


class Expr(Comparable):

    __slots__ = ('_sql', '_params')

    def __init__(self, sql, *params):
        if params and is_list(params[0]):
            return self.__init__(sql, *params[0])
        self._sql, self._params = sql, params


@compile.when(Expr)
def compile_expr(compile, expr, state):
    state.sql.append(expr._sql)
    state.params += expr._params


class MetaCompositeExpr(type):

    def __new__(cls, name, bases, attrs):
        if bases[0] is object:
            def _c(name):
                def f(self, other):
                    if hasattr(operator, name):
                        return reduce(operator.and_, (getattr(operator, name)(expr, val) for (expr, val) in zip(self.data, other)))
                    else:
                        return reduce(operator.and_, (getattr(expr, name)(val) for (expr, val) in zip(self.data, other)))
                return f

            for a in ('__eq__', '__neg__'):
                attrs[a] = _c(a)
        return type.__new__(cls, name, bases, attrs)


class CompositeExpr(MetaCompositeExpr("NewBase", (object, ), {})):

    __slots__ = ('data', '_sql')

    def __init__(self, *args):
        self.data = args
        self._sql = ", "

    def as_(self, aliases):
        return self.__class__(*(expr.as_(alias) for expr, alias in zip(self.data, aliases)))

    def in_(self, others):
        if len(self.data) == 1:
            return self.data[0].in_(others)
        return reduce(operator.or_,
                      (reduce(operator.and_,
                              ((expr == other)
                               for expr, other in zip(self.data, composite_other)))
                       for composite_other in others))

    def not_in(self, others):
        if len(self.data) == 1:
            return self.data[0].not_in(others)
        return ~reduce(operator.or_,
                       (reduce(operator.and_,
                               ((expr == other)
                                for expr, other in zip(self.data, composite_other)))
                        for composite_other in others))

    def __iter__(self):
        return iter(self.data)


@compile.when(CompositeExpr)
def compile_compositeexpr(compile, expr, state):
    state.push('callers')
    state.callers.pop(0)  # pop CompositeExpr from caller's stack to correct render of aliases.
    compile_exprlist(compile, expr, state)
    state.pop()


class Binary(Expr):
    __slots__ = ('_left', '_right')

    def __init__(self, left, op, right):
        self._left = left
        self._sql = op.upper()
        self._right = right

Condition = Binary


@compile.when(Binary)
def compile_condition(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(SPACE)
    state.sql.append(expr._sql)
    state.sql.append(SPACE)
    compile(expr._right, state)


class NamedBinary(Binary):
    __slots__ = ()

    def __init__(self, left, right):
        # Don't use multi-arguments form like And(*args)
        # Use reduce(operator.and_, args) or reduce(And, args) instead. SRP.
        self._left = left
        self._right = right

NamedCondition = NamedBinary


class NamedCompound(NamedBinary):
    __slots__ = ()

    def __init__(self, *exprs):
        self._left = reduce(self.__class__, exprs[:-1])
        self._right = exprs[-1]


class Add(NamedCompound):
    _sql = '+'


class Sub(NamedBinary):
    __slots__ = ()
    _sql = '-'


class Mul(NamedCompound):
    __slots__ = ()
    _sql = '*'


class Div(NamedBinary):
    __slots__ = ()
    _sql = '/'


class Gt(NamedBinary):
    __slots__ = ()
    _sql = '>'


class Lt(NamedBinary):
    __slots__ = ()
    _sql = '<'


class Ge(NamedBinary):
    __slots__ = ()
    _sql = '>='


class Le(NamedBinary):
    __slots__ = ()
    _sql = '<='


class And(NamedCompound):
    __slots__ = ()
    _sql = 'AND'


class Or(NamedCompound):
    __slots__ = ()
    _sql = 'OR'


class Eq(NamedBinary):
    __slots__ = ()
    _sql = '='


class Ne(NamedBinary):
    __slots__ = ()
    _sql = '<>'


class Is(NamedBinary):
    __slots__ = ()
    _sql = 'IS'


class IsNot(NamedBinary):
    __slots__ = ()
    _sql = 'IS NOT'


class In(NamedBinary):
    __slots__ = ()
    _sql = 'IN'


class NotIn(NamedBinary):
    __slots__ = ()
    _sql = 'NOT IN'


class RShift(NamedBinary):
    __slots__ = ()
    _sql = ">>"


class LShift(NamedBinary):
    __slots__ = ()
    _sql = "<<"


class EscapeForLike(Expr):

    __slots__ = ('_expr')

    _escape = "!"
    _escape_map = tuple(  # Ordering is important!
        (i, "!{0}".format(i)) for i in ('!', '_', '%')
    )

    def __init__(self, expr):
        self._expr = expr


@compile.when(EscapeForLike)
def compile_escapeforlike(compile, expr, state):
    escaped = expr._expr
    for k, v in expr._escape_map:
        escaped = Replace(escaped, Value(k), Value(v))
    compile(escaped, state)


class Like(NamedBinary):
    __slots__ = ('_escape',)
    _sql = 'LIKE'

    def __init__(self, left, right, escape=Undef):
        self._left = left
        self._right = right
        if isinstance(right, EscapeForLike):
            self._escape = right._escape
        else:
            self._escape = escape


class Ilike(Like):
    __slots__ = ()
    _sql = 'ILIKE'


@compile.when(Like)
def compile_like(compile, expr, state):
    compile_condition(compile, expr, state)
    if expr._escape is not Undef:
        state.sql.append(' ESCAPE ')
        compile(Value(expr._escape) if isinstance(expr._escape, string_types) else expr._escape, state)


class ExprList(Expr):

    __slots__ = ('data', )

    def __init__(self, *args):
        # if args and is_list(args[0]):
        #     return self.__init__(*args[0])
        self._sql, self.data = " ", list(args)

    def join(self, sep):
        self._sql = sep
        return self

    def __len__(self):
        return len(self.data)

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return ExprList(*self.data[start:end])
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def append(self, x):
        return self.data.append(x)

    def insert(self, i, x):
        return self.data.insert(i, x)

    def extend(self, L):
        return self.data.extend(L)

    def pop(self, i):
        return self.data.pop(i)

    def remove(self, x):
        return self.data.remove(x)

    def reset(self):
        del self.data[:]
        return self

    def __copy__(self):
        dup = copy.copy(super(ExprList, self))
        dup.data = dup.data[:]
        return dup


@compile.when(ExprList)
def compile_exprlist(compile, expr, state):
    first = True
    for a in expr:
        if first:
            first = False
        else:
            state.sql.append(expr._sql)
        compile(a, state)


class FieldList(ExprList):
    __slots__ = ()

    def __init__(self, *args):
        # if args and is_list(args[0]):
        #     return self.__init__(*args[0])
        self._sql, self.data = ", ", list(args)


@compile.when(FieldList)
def compile_fieldlist(compile, expr, state):
    # state.push('context', CONTEXT_COLUMN)
    compile_exprlist(compile, expr, state)
    # state.pop()


class Concat(ExprList):

    __slots__ = ('_ws', )

    def __init__(self, *args):
        super(Concat, self).__init__(*args)
        self._sql = ' || '
        self._ws = None

    def ws(self, sep):
        self._ws = sep
        self._sql = ', '
        return self


class Array(ExprList):
    __slots__ = ()

    def __init__(self, *args):
        self._sql, self.data = ", ", list(args)


@compile.when(Array)
def compile_array(compile, expr, state):
    if not expr.data:
        state.sql.append("'{}'")
    state.sql.append("ARRAY[{0}]".format(compile_exprlist(compile, expr, state)))


@compile.when(Concat)
def compile_concat(compile, expr, state):
    if not expr._ws:
        return compile_exprlist(compile, expr, state)
    state.sql.append('concat_ws(')
    compile(expr._ws, state)
    for a in expr:
        state.sql.append(expr._sql)
        compile(a, state)
    state.sql.append(')')


class Param(Expr):

    __slots__ = ()

    def __init__(self, params):
        self._params = params


@compile.when(Param)
def compile_param(compile, expr, state):
    compile(expr._params, state)


Placeholder = Param


class Parentheses(Expr):

    __slots__ = ('_expr', )

    def __init__(self, expr):
        self._expr = expr


@compile.when(Parentheses)
def compile_parentheses(compile, expr, state):
    state.precedence += MAX_PRECEDENCE
    compile(expr._expr, state)


class OmitParentheses(Parentheses):
    pass


@compile.when(OmitParentheses)
def compile_omitparentheses(compile, expr, state):
    state.precedence = 0
    compile(expr._expr, state)


class Prefix(Expr):

    __slots__ = ('_expr', )

    def __init__(self, prefix, expr):
        self._sql = prefix
        self._expr = expr


@compile.when(Prefix)
def compile_prefix(compile, expr, state):
    state.sql.append(expr._sql)
    state.sql.append(SPACE)
    compile(expr._expr, state)


class NamedPrefix(Prefix):
    __slots__ = ()

    def __init__(self, expr):
        self._expr = expr


class Not(NamedPrefix):
    __slots__ = ()
    _sql = 'NOT'


class All(NamedPrefix):
    __slots__ = ()
    _sql = 'All'


class Distinct(NamedPrefix):
    __slots__ = ()
    _sql = 'DISTINCT'


class Exists(NamedPrefix):
    __slots__ = ()
    _sql = 'EXISTS'


class Unary(Prefix):
    __slots__ = ()


@compile.when(Unary)
def compile_unary(compile, expr, state):
    state.sql.append(expr._sql)
    compile(expr._expr, state)


class NamedUnary(Unary):
    __slots__ = ()

    def __init__(self, expr):
        self._expr = expr


class Pos(NamedUnary):
    __slots__ = ()
    _sql = '+'


class Neg(NamedUnary):
    __slots__ = ()
    _sql = '-'


class Postfix(Expr):
    __slots__ = ('_expr', )

    def __init__(self, expr, postfix):
        self._sql = postfix
        self._expr = expr


@compile.when(Postfix)
def compile_postfix(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append(SPACE)
    state.sql.append(expr._sql)


class NamedPostfix(Postfix):
    __slots__ = ()

    def __init__(self, expr):
        self._expr = expr


class OrderDirection(NamedPostfix):
    __slots__ = ()

    def __init__(self, expr):
        if isinstance(expr, OrderDirection):
            expr = expr._expr
        self._expr = expr


class Asc(OrderDirection):
    __slots__ = ()
    _sql = 'ASC'


class Desc(OrderDirection):
    __slots__ = ()
    _sql = 'DESC'


class Between(Expr):

    __slots__ = ('_expr', '_start', '_end')

    def __init__(self, expr, start, end):
        self._expr, self._start, self._end = expr, start, end


@compile.when(Between)
def compile_between(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append(' BETWEEN ')
    compile(expr._start, state)
    state.sql.append(' AND ')
    compile(expr._end, state)


class Case(Expr):
    """A CASE statement.

    @params cases: a list of tuples of (condition, result) or (value, result),
        if an expression is passed too.
    @param expression: the expression to compare (if the simple form is used).
    @param default: an optional default condition if no other case matches.
    """

    __slots__ = ('_cases', '_expr', '_default')

    def __init__(self, cases, expr=Undef, default=Undef):
        self._cases = cases
        self._expr = expr
        self._default = default


@compile.when(Case)
def compile_case(compile, expr, state):
    state.sql.append('CASE')
    if expr._expr is not Undef:
        state.sql.append(SPACE)
        compile(expr._expr, state)
    for clouse, value in expr._cases:
        state.sql.append(' WHEN ')
        compile(clouse, state)
        state.sql.append(' THEN ')
        compile(value, state)
    if expr._default is not Undef:
        state.sql.append(' ELSE ')
        compile(expr._default, state)
    state.sql.append(' END ')


class Callable(Expr):

    __slots__ = ('_expr', '_args')

    def __init__(self, expr, *args):
        self._expr = expr
        self._args = ExprList(*args).join(", ")


@compile.when(Callable)
def compile_callable(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append('(')
    compile(expr._args, state)
    state.sql.append(')')


class NamedCallable(Callable):
    __slots__ = ()

    def __init__(self, *args):
        self._args = ExprList(*args).join(", ")


@compile.when(NamedCallable)
def compile_namedcallable(compile, expr, state):
    state.sql.append(expr._sql)
    state.sql.append('(')
    compile(expr._args, state)
    state.sql.append(')')


class Replace(NamedCallable):
    __slots__ = ()
    _sql = 'REPLACE'


class Constant(Expr):

    __slots__ = ()

    def __init__(self, const):
        self._sql = const.upper()

    def __call__(self, *args):
        return Callable(self, *args)


@compile.when(Constant)
def compile_constant(compile, expr, state):
    state.sql.append(expr._sql)


class ConstantSpace(object):

    __slots__ = ()

    def __getattr__(self, attr):
        return Constant(attr)


class MetaField(type):

    def __getattr__(cls, key):
        if key[0] == '__':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 2)
        prefix, name, alias = parts + [None] * (3 - len(parts))
        if name is None:
            prefix, name = name, prefix
        f = cls(name, prefix)
        return f.as_(alias) if alias else f


class Field(MetaField("NewBase", (Expr,), {})):
    # It's a field, not column, because prefix can be alias of subquery.
    # It also can be a field of composite column.
    __slots__ = ('_name', '_prefix', '__cached__')

    def __init__(self, name, prefix=None):
        if isinstance(name, string_types):
            if name == '*':
                name = Constant(name)
            else:
                name = Name(name)
        self._name = name
        if isinstance(prefix, string_types):
            prefix = Table(prefix)
        self._prefix = prefix
        self.__cached__ = {}


@compile.when(Field)
@cached_compile
def compile_field(compile, expr, state):
    if expr._prefix is not None:
        state.auto_tables.append(expr._prefix)  # it's important to know the concrete alias of table.
        compile(expr._prefix, state)
        state.sql.append('.')
    compile(expr._name, state)


class Subfield(Expr):

    __slots__ = ('_parent', '_name')

    def __init__(self, parent, name):
        self._parent = parent
        if isinstance(name, string_types):
            name = Name(name)
        self._name = name


@compile.when(Subfield)
def compile_subfield(compile, expr, state):
    parent = expr._parent
    if True:  # get me from context
        parent = Parentheses(parent)
    compile(parent)
    state.sql.append('.')
    compile(expr._name, state)


class ArrayItem(Expr):

    __slots__ = ('_array', '_key')

    def __init__(self, array, key):
        self._array = array
        assert isinstance(key, slice)
        self._key = key


@compile.when(ArrayItem)
def compile_arrayitem(compile, expr, state):
    compile(expr._array)
    state.sql.append("[")
    state.sql.append("{0:d}".format(expr.start))
    if expr.stop is not None:
        state.sql.append(", ")
        state.sql.append("{0:d}".format(expr.stop))
    state.sql.append("]")


class Alias(Expr):

    __slots__ = ('_expr', '_sql')

    def __init__(self, alias, expr=None):
        self._expr = expr
        if isinstance(alias, string_types):
            alias = Name(alias)
        super(Alias, self).__init__(alias)


@compile.when(Alias)
def compile_alias(compile, expr, state):
    try:
        render_column = issubclass(state.callers[1], FieldList)
        # render_column = state.context == CONTEXT_COLUMN
    except IndexError:
        pass
    else:
        if render_column:
            compile(expr._expr, state)
            state.sql.append(' AS ')
    compile(expr._sql, state)


class MetaTableSpace(type):

    def __instancecheck__(cls, instance):
        return isinstance(instance, Table)

    def __subclasscheck__(cls, subclass):
        return issubclass(subclass, Table)

    def __getattr__(cls, key):
        if key.startswith('__'):
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        table = Table(name)
        return table.as_(alias) if alias else table

    def __call__(cls, name, *a, **kw):
        return Table(name, *a, **kw)


class T(MetaTableSpace("NewBase", (object, ), {})):
    pass


class MetaTable(type):

    def __new__(cls, name, bases, attrs):
        if bases[0] is object:
            def _f(attr):
                return lambda self, *a, **kw: getattr(TableJoin(self), attr)(*a, **kw)

            for a in ['inner_join', 'left_join', 'right_join', 'full_join', 'cross_join',
                      'join', 'on', 'hint', 'natural', 'using']:
                attrs[a] = _f(a)
        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key[0] == '__':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        table = cls(name)
        return table.as_(alias) if alias else table


class FieldProxy(object):

    def __init__(self, table):
        self.__table = table

    def __getattr__(self, key):
        if key[:2] == '__':
            raise AttributeError
        return self.__table.get_field(key)

    __call__ = __getattr__
    __getitem__ = __getattr__

# TODO: Schema support. Not only for table.
# A database contains one or more named schemas, which in turn contain tables. Schemas also contain other kinds of named objects, including data types, functions, and operators.
# http://www.postgresql.org/docs/9.4/static/ddl-schemas.html
# Ideas: S.public(T.user), S('public', T.user)


class Table(MetaTable("NewBase", (object, ), {})):
    # Variants:
    # tb.as_ => Field(); tb().as_ => instancemethod() ???
    # author + InnerJoin + book + On + author.id == book.author_id
    # Add __call__() method to Field/Alias
    # Use sys._getframe(), compiler.visitor.ASTVisitor or tokenize.generate_tokens() to get context for Table.__getattr__()

    __slots__ = ('_name', '__cached__', 'f')

    def __init__(self, name):
        if isinstance(name, string_types):
            name = Name(name)
        self._name = name
        self.__cached__ = {}
        self.f = FieldProxy(self)

    def as_(self, alias):
        return TableAlias(alias, self)

    def __getattr__(self, key):
        if key[0] == '__':
            raise AttributeError
        return self.get_field(key)

    def __getitem__(self, key):
        return self.get_field(key)

    def get_field(self, key):
        cache = self.f.__dict__
        if key in cache:
            return cache[key]

        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))

        if name in cache:
            f = cache[name]
        else:
            f = Field(name, self)
            cache[name] = f
        if alias:
            f = f.as_(alias)
            cache[key] = f
        return f

    __and__ = same('inner_join')
    __add__ = same('left_join')
    __sub__ = same('right_join')
    __or__ = same('full_join')
    __mul__ = same('cross_join')


@compile.when(Table)
def compile_table(compile, expr, state):
    compile(expr._name, state)


class TableAlias(Table):

    __slots__ = ('_table', '_alias', 'fields')

    def __init__(self, alias, table=None):
        if isinstance(alias, string_types):
            alias = Name(alias)
        self._alias = alias
        self._table = table
        self.__cached__ = {}
        self.f = FieldProxy(self)

    def as_(self, alias):
        return type(self)(alias, self._table)


@compile.when(TableAlias)
def compile_tablealias(compile, expr, state):
    # if expr._table is not None and state.context == CONTEXT_TABLE:
    try:
        render_table = expr._table is not None and issubclass(state.callers[1], TableJoin)
        # render_table = expr._table is not None and state.context == CONTEXT_TABLE
    except IndexError:
        pass
    else:
        if render_table:
            compile(expr._table, state)
            state.sql.append(' AS ')
    compile(expr._alias, state)


class TableJoin(object):

    __slots__ = ('_table', '_alias', '_join_type', '_on', '_left', '_hint', '_nested', '_natural', '_using')

    # TODO: support for ONLY http://www.postgresql.org/docs/9.4/static/tutorial-inheritance.html

    def __init__(self, table_or_alias, join_type=None, on=None, left=None):
        self._table = table_or_alias
        self._join_type = join_type
        self._on = on
        self._left = left
        self._hint = None
        self._nested = False
        self._natural = False
        self._using = None

    def _j(j):
        return lambda self, obj: self.join(j, obj)

    inner_join = _j("INNER JOIN")
    left_join = _j("LEFT OUTER JOIN")
    right_join = _j("RIGHT OUTER JOIN")
    full_join = _j("FULL OUTER JOIN")
    cross_join = _j("CROSS JOIN")

    def join(self, join_type, obj):
        if not isinstance(obj, TableJoin) or obj.left():
            obj = type(self)(obj, left=self)
        obj = obj.left(self).join_type(join_type)
        return obj

    def left(self, left=None):
        if left is None:
            return self._left
        self._left = left
        return self

    def join_type(self, join_type):
        self._join_type = join_type
        return self

    def on(self, c):
        if self._on is not None:
            self = self.__class__(self)  # TODO: Test me.
        self._on = c
        return self

    def natural(self):
        self._natural = True
        return self

    def using(self, *fields):
        self._using = ExprList(*fields).join(", ")
        return self

    def __call__(self):
        self._nested = True
        self = self.__class__(self)
        return self

    def hint(self, expr):
        if isinstance(expr, string_types):
            expr = Expr(expr)
        self._hint = OmitParentheses(expr)
        return self

    def __copy__(self):
        dup = copy.copy(super(TableJoin, self))
        for a in ['_hint', ]:
            setattr(dup, a, copy.copy(getattr(dup, a, None)))
        return dup

    as_nested = same('__call__')
    group = same('__call__')
    __and__ = same('inner_join')
    __add__ = same('left_join')
    __sub__ = same('right_join')
    __or__ = same('full_join')
    __mul__ = same('cross_join')


@compile.when(TableJoin)
def compile_tablejoin(compile, expr, state):
    if expr._nested:
        state.sql.append('(')
    if expr._left is not None:
        compile(expr._left, state)
    if expr._join_type:
        state.sql.append(SPACE)
        if expr._natural:
            state.sql.append('NATURAL ')
        state.sql.append(expr._join_type)
        state.sql.append(SPACE)
    state.push('context', CONTEXT_TABLE)
    compile(expr._table, state)
    state.pop()
    if expr._on is not None:
        state.sql.append(' ON ')
        compile(expr._on, state)
    elif expr._using is not None:
        state.sql.append(' USING ')
        compile(expr._using, state)
    if expr._hint is not None:
        state.sql.append(SPACE)
        compile(expr._hint, state)
    if expr._nested:
        state.sql.append(')')


class Result(object):
    """Default implementation of Query class.

    It uses the Bridge pattern to separate implementation from interface.
    """

    compile = compile

    def __init__(self, compile=None):
        if compile is not None:
            self.compile = compile

    def execute(self):
        return self.compile(self._query)

    select = count = insert = update = delete = execute

    def __call__(self, query):
        c = self  # Don't clone here to keep link to cache in this instance
        c._query = query
        return c

    def clone(self):
        c = copy.copy(super(Result, self))
        c._query = None
        return c

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        if isinstance(key, slice):
            offset = key.start or 0
            limit = key.stop - offset if key.stop else None
        else:
            offset, limit = key, 1
        return self._query.limit(offset, limit)

    __copy__ = clone


class Query(Expr):
    # Without methods like insert, delete, update etc. it will be named Select.

    result = Result()

    def __init__(self, tables=None, result=None):
        """ Query class.

        It uses the Bridge pattern to separate implementation from interface.

        :param tables: tables
        :type tables: Table, TableAlias, TableJoin or None
        :param result: Object of implementation.
        :type tables: Result
        """
        self._distinct = ExprList().join(", ")
        self._fields = FieldList().join(", ")
        if tables is not None:
            if not isinstance(tables, TableJoin):
                tables = TableJoin(tables)
        self._tables = tables
        self._wheres = None
        self._havings = None
        self._group_by = ExprList().join(", ")
        self._order_by = ExprList().join(", ")
        self._limit = None
        self._offset = None
        self._for_update = False

        if result is not None:
            self.result = result
        else:
            self.result = self.result.clone()

    def clone(self, *attrs):
        c = copy.copy(super(Query, self))
        # if not attrs:
        #     attrs = ('_fields', '_tables', '_group_by', '_order_by')
        for a in attrs:
            setattr(c, a, copy.copy(getattr(c, a, None)))
        c.result = c.result.clone()
        return c

    def tables(self, tables=None):
        if tables is None:
            return self._tables
        self = self.clone('_tables')
        self._tables = tables if isinstance(tables, TableJoin) else TableJoin(tables)
        return self

    @opt_checker(["reset", ])
    def distinct(self, *args, **opts):
        if not args and not opts:
            return self._distinct

        if args:
            if is_list(args[0]):
                return self.distinct(*args[0], reset=True)
            elif args[0] is True and not opts.get("reset"):
                return self.distinct(*args, reset=True)
            elif args[0] is False:
                return self.distinct(reset=True)

        c = self.clone('_distinct')
        if opts.get("reset"):
            c._distinct.reset()
        if args:
            c._distinct.extend(args)
        return c

    @opt_checker(["reset", ])
    def fields(self, *args, **opts):
        # Why not name the "args" by "expressions", or "exprs" or "fields"?
        # Because it wil be a lie. The argument can be a list of expressions.
        # The name "argument" is not entirely clear, but truthful and not misleading.
        if not args and not opts:
            return self._fields

        if args and is_list(args[0]):
            return self.fields(*args[0], reset=True)

        c = self.clone('_fields')
        if opts.get("reset"):
            c._fields.reset()
        if args:
            c._fields.extend([Field(f) if isinstance(f, string_types) else f for f in args])
        return c

    def on(self, cond):
        # TODO: Remove?
        self = self.clone()
        if not isinstance(self._tables, TableJoin):
            raise Error("Can't set on without join table")
        self._tables = self._tables.on(cond)
        return self

    def where(self, cond, op=operator.and_):
        self = self.clone()
        self._wheres = cond if self._wheres is None or op is None else op(self._wheres, cond)
        return self

    def or_where(self, cond):
        warn('or_where(cond)', 'where(cond, op=operator.or_)')
        return self.where(cond, op=operator.or_)

    @opt_checker(["reset", ])
    def group_by(self, *args, **opts):
        if not args and not opts:
            return self._group_by

        if args and is_list(args[0]):
            return self.group_by(*args[0], reset=True)

        c = self.clone('_group_by')
        if opts.get("reset"):
            c._group_by.reset()
        if args:
            c._group_by.extend(args)
        return c

    def having(self, cond, op=operator.and_):
        c = self.clone()
        c._havings = cond if c._havings is None or op is None else op(self._havings, cond)
        return c

    def or_having(self, cond):
        warn('or_having(cond)', 'having(cond, op=operator.or_)')
        return self.having(cond, op=operator.or_)

    @opt_checker(["desc", "reset", ])
    def order_by(self, *args, **opts):
        if not args and not opts:
            return self._order_by

        if args and is_list(args[0]):
            return self.order_by(*args[0], reset=True)

        c = self.clone('_order_by')
        if opts.get("reset"):
            c._order_by.reset()
        if args:
            wraps = Desc if opts.get("desc") else Asc
            c._order_by.extend([f if isinstance(f, (Asc, Desc)) else wraps(f) for f in args])
        return c

    def limit(self, *args, **kwargs):
        c = self.clone()
        if args:
            if len(args) < 2:
                args = (0,) + args
            c._offset, c._limit = args
        else:
            c._limit = kwargs.get('limit')
            c._offset = kwargs.get('offset', 0)
        return c

    @opt_checker(["distinct", "for_update"])
    def select(self, *args, **opts):
        c = self.clone()
        if args:
            c = c.fields(*args)
        if opts.get("distinct"):
            c = c.distinct(True)
        if opts.get("for_update"):
            c._for_update = True
        return c.result(c).select()
        # Never do clone result. It should to have back link to Query instance.
        # State of Result should be corresponding to state of Query object.
        # We need clone both Result and Query synchronously.

    def count(self, **kw):
        return self.result(SelectCount(self, **kw)).count()

    def insert(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        return self.result(Insert(map=key_values, **kw)).insert()

    def insert_many(self, fields, values, **kw):
        # Deprecated
        return self.insert(fields=fields, values=values, **kw)

    def update(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        kw.setdefault('where', self._wheres)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(Update(map=key_values, **kw)).update()

    def delete(self, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('where', self._wheres)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(Delete(**kw)).delete()

    def as_table(self, alias):
        return TableAlias(alias, self)

    def as_set(self, all=False):
        return Set(self, all=all, result=self.result)

    def raw(self, sql, params=()):
        return Raw(sql, params, result=self.result)

    def result_wraps(self, name, *args, **kwargs):
        """Wrapper to call implementation method."""
        c = self.clone()
        return getattr(c.result(c), name)(*args, **kwargs)

    def __getitem__(self, key):
        return self.result(self).__getitem__(key)

    def __len__(self):
        return self.result(self).__len__()

    def __iter__(self):
        return self.result(self).__iter__()

    def __getattr__(self, name):
        """Delegates unknown attributes to object of implementation."""
        if hasattr(self.result, name):
            attr = getattr(self.result, name)
            if isinstance(attr, types.MethodType):
                return partial(self.result_wraps, name)
            else:
                return attr
        raise AttributeError

    def set(self, *args, **kwargs):
        warn('set([all=False])', 'as_set([all=False])')
        return self.as_set(*args, **kwargs)

    columns = same('fields')
    __copy__ = same('clone')


QuerySet = Query


@compile.when(Query)
def compile_query(compile, expr, state):
    state.push("auto_tables", [])  # this expr can be a subquery
    state.sql.append("SELECT ")
    if expr._distinct:
        state.sql.append("DISTINCT ")
        if expr._distinct[0] is not True:
            state.sql.append("ON ")
            compile(Parentheses(expr._distinct), state)
            state.sql.append(SPACE)
    compile(expr._fields, state)

    tables_sql_pos = len(state.sql)
    tables_params_pos = len(state.params)

    if expr._wheres:
        state.sql.append(" WHERE ")
        compile(expr._wheres, state)
    if expr._group_by:
        state.sql.append(" GROUP BY ")
        compile(expr._group_by, state)
    if expr._havings:
        state.sql.append(" HAVING ")
        compile(expr._havings, state)
    if expr._order_by:
        state.sql.append(" ORDER BY ")
        compile(expr._order_by, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)
    if expr._for_update:
        state.sql.append(" FOR UPDATE")

    state.push('join_tables', [])
    state.push('sql', [])
    state.push('params', [])
    state.sql.append(" FROM ")
    compile(expr._tables, state)
    tables_sql = state.sql
    tables_params = state.params
    state.pop()
    state.pop()
    state.pop()
    state.sql[tables_sql_pos:tables_sql_pos] = tables_sql
    state.params[tables_params_pos:tables_params_pos] = tables_params
    state.pop()


class SelectCount(Query):

    def __init__(self, q, table_alias='count_list', field_alias='count_value'):
        Query.__init__(self, q.order_by(reset=True).as_table(table_alias))
        self._fields.append(Constant('COUNT')(Constant('1')).as_(field_alias))


class Raw(Query):

    def __init__(self, sql, params, result=None):
        Query.__init__(self, result=result)
        self._raw = OmitParentheses(Expr(sql, params))


@compile.when(Raw)
def compile_raw(compile, expr, state):
    compile(expr._raw, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)


class Modify(object):
    pass


class Insert(Modify):

    def __init__(self, table, map=None, fields=None, values=None, ignore=False, on_duplicate_key_update=None):
        self._table = table
        self._fields = FieldList(*(k if isinstance(k, Expr) else Field(k) for k in (map or fields)))
        self._values = (tuple(map.values()),) if map else values
        self._ignore = ignore
        self._on_duplicate_key_update = tuple(
            (k if isinstance(k, Expr) else Field(k), v)
            for k, v in on_duplicate_key_update.items()
        ) if on_duplicate_key_update else None


@compile.when(Insert)
def compile_insert(compile, expr, state):
    state.sql.append("INSERT ")
    if expr._ignore:
        state.sql.append("IGNORE ")
    state.sql.append("INTO ")
    compile(expr._table, state)
    state.sql.append(SPACE)
    compile(Parentheses(expr._fields), state)
    if isinstance(expr._values, Query):
        state.sql.append(SPACE)
        compile(expr._values, state)
    else:
        state.sql.append(" VALUES ")
        compile(ExprList(*expr._values).join(', '), state)
    if expr._on_duplicate_key_update:
        state.sql.append(" ON DUPLICATE KEY UPDATE ")
        first = True
        for f, v in expr._on_duplicate_key_update:
            if first:
                first = False
            else:
                state.sql.append(", ")
            compile(f, state)
            state.sql.append(" = ")
            compile(v, state)


class Update(Modify):

    def __init__(self, table, map=None, fields=None, values=None, ignore=False, where=None, order_by=None, limit=None):
        self._table = table
        self._fields = FieldList(*(k if isinstance(k, Expr) else Field(k) for k in (map or fields)))
        self._values = tuple(map.values()) if map else values
        self._ignore = ignore
        self._where = where
        self._order_by = order_by
        self._limit = limit


@compile.when(Update)
def compile_update(compile, expr, state):
    state.sql.append("UPDATE ")
    if expr._ignore:
        state.sql.append("IGNORE ")
    compile(expr._table, state)
    state.sql.append(" SET ")
    first = True
    for f, v in zip(expr._fields, expr._values):
        if first:
            first = False
        else:
            state.sql.append(", ")
        compile(f, state)
        state.sql.append(" = ")
        compile(v, state)
    if expr._where:
        state.sql.append(" WHERE ")
        compile(expr._where, state)
    if expr._order_by:
        state.sql.append(" ORDER BY ")
        compile(expr._order_by, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)


class Delete(Modify):

    def __init__(self, table, where=None, order_by=None, limit=None):
        self._table = table
        self._where = where
        self._order_by = order_by
        self._limit = limit


@compile.when(Delete)
def compile_delete(compile, expr, state):
    state.sql.append("DELETE FROM ")
    compile(expr._table, state)
    if expr._where:
        state.sql.append(" WHERE ")
        compile(expr._where, state)
    if expr._order_by:
        state.sql.append(" ORDER BY ")
        compile(expr._order_by, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)


class Set(Query):

    def __init__(self, *exprs, **kw):
        super(Set, self).__init__()
        if 'op' in kw:
            self._sql = kw['op']
        self._all = kw.get('all', False)  # Use All() instead?
        self._exprs = ExprList()
        for expr in exprs:
            self.add(expr)
        if 'result' in kw:
            self.result = kw['result']
        else:
            self.result = self.result.clone()

    def add(self, other):
        if (isinstance(other, self.__class__) and
                other._all == self._all and
                other._limit is None and
                other._offset is None):
            for expr in other._exprs:
                self.add(expr)
            if other._for_update:
                self._for_update = other._for_update
        else:
            self._exprs.append(other)
            # TODO: reset _order_by, _for_update?

    def _op(self, cls, other):
        if not getattr(self, '_sql', None):
            c = cls(*self._exprs, all=self._all)
            c._limit = self._limit
            c._offset = self._offset
            c._order_by = self._order_by
            c._for_update = self._for_update
        elif self.__class__ is not cls:
            c = cls(self, all=self._all)  # TODO: Should be here "all"?
        else:
            c = self.clone()
        c.add(other)
        return c

    def __or__(self, other):
        return self._op(Union, other)

    def __and__(self, other):
        return self._op(Intersect, other)

    def __sub__(self, other):
        return self._op(Except, other)

    def all(self, all=True):
        self._all = all
        return self

    def clone(self, *attrs):
        self = Query.clone(self, *attrs)
        self._exprs = copy.copy(self._exprs)
        return self


class Union(Set):
    __slots__ = ()
    _sql = 'UNION'


class Intersect(Set):
    __slots__ = ()
    _sql = 'INTERSECT'


class Except(Set):
    __slots__ = ()
    _sql = 'EXCEPT'


@compile.when(Set)
def compile_set(compile, expr, state):
    if expr._all:
        op = ' {0} ALL '.format(expr._sql)
    else:
        op = ' {0} '.format(expr._sql)
    # TODO: add tests for nested sets.
    state.precedence += 0.5  # to correct handle sub-set with limit, offset
    compile(expr._exprs.join(op), state)
    state.precedence -= 0.5
    if expr._order_by:
        state.sql.append(" ORDER BY ")
        compile(expr._order_by, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)
    if expr._for_update:
        state.sql.append(" FOR UPDATE")


class Name(object):

    __slots__ = ('_name', )

    def __init__(self, name=None):
        self._name = name


class NameCompiler(object):

    _translation_map = (
        ("\\", "\\\\"),
        ("\000", "\\0"),
        ('\b', '\\b'),
        ('\n', '\\n'),
        ('\r', '\\r'),
        ('\t', '\\t'),
        ("%", "%%")
    )
    _delimeter = '"'
    _escape_delimeter = '"'
    _max_length = 63

    class MaxLengthError(Error):
        pass

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_{}'.format(k), v)

    def __call__(self, compile, expr, state):
        state.sql.append(self._delimeter)
        name = expr._name
        name = name.replace(self._delimeter, self._escape_delimeter + self._delimeter)
        for k, v in self._translation_map:
            name = name.replace(k, v)
        if len(name) > self._get_max_length(state):
            raise self.MaxLengthError("The length of name {0!r} is more than {1}".format(name, self._max_length))
        state.sql.append(name)
        state.sql.append(self._delimeter)

    def _get_max_length(self, state):
        # Max length can depend on context.
        return self._max_length

compile_name = NameCompiler()
compile.when(Name)(compile_name)


class Value(object):

    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


class ValueCompiler(object):

    _translation_map = (
        ("\\", "\\\\"),
        ("\000", "\\0"),
        ('\b', '\\b'),
        ('\n', '\\n'),
        ('\r', '\\r'),
        ('\t', '\\t'),
        ("%", "%%")
    )
    _delimeter = "'"
    _escape_delimeter = "'"

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_{}'.format(k), v)

    def __call__(self, compile, expr, state):
        state.sql.append(self._delimeter)
        value = str(expr._value)
        value = value.replace(self._delimeter, self._escape_delimeter + self._delimeter)
        for k, v in self._translation_map:
            value = value.replace(k, v)
        state.sql.append(value)
        state.sql.append(self._delimeter)

compile_value = ValueCompiler()
compile.when(Value)(compile_value)


def is_list(v):
    return isinstance(v, (list, tuple))


def is_allowed_attr(instance, key):
    if key.startswith('__'):
        return False
    if key in dir(instance.__class__):  # type(instance)?
        # It's a descriptor, like '_sql' defined in slots
        return False
    return True


def warn(old, new, stacklevel=3):
    warnings.warn("{0} is deprecated. Use {1} instead".format(old, new), PendingDeprecationWarning, stacklevel=stacklevel)

compile.set_precedence(270, '.')
compile.set_precedence(260, '::')
compile.set_precedence(250, '[', ']')  # array element selection
compile.set_precedence(240, Pos, Neg, (Unary, '+'), (Unary, '-'), '~')  # unary minus
compile.set_precedence(230, '^')
compile.set_precedence(220, Mul, Div, (Binary, '*'), (Binary, '/'), (Binary, '%'))
compile.set_precedence(210, Add, Sub, (Binary, '+'), (Binary, '-'))
compile.set_precedence(200, LShift, RShift, '<<', '>>')
compile.set_precedence(190, '&')
compile.set_precedence(185, '#')
compile.set_precedence(180, '|')
compile.set_precedence(170, Is, 'IS')
compile.set_precedence(160, (Postfix, 'ISNULL'))
compile.set_precedence(150, (Postfix, 'NOTNULL'))
compile.set_precedence(140, '(any other)')  # all other native and user-defined operators
compile.set_precedence(130, In, NotIn, 'IN')
compile.set_precedence(120, Between, 'BETWEEN')
compile.set_precedence(110, 'OVERLAPS')
compile.set_precedence(100, Like, Ilike, 'LIKE', 'ILIKE', 'SIMILAR')
compile.set_precedence(90, Lt, Gt, '<', '>')
compile.set_precedence(80, Le, Ge, Ne, '<=', '>=', '<>', '!=')
compile.set_precedence(70, Eq, '=')
compile.set_precedence(60, Not, 'NOT')
compile.set_precedence(50, And, 'AND')
compile.set_precedence(40, Or, 'OR')
compile.set_precedence(30, Set, Union, Intersect, Except)
compile.set_precedence(20, Query, SelectCount, Raw, Insert, Update, Delete)
compile.set_precedence(10, Expr)
compile.set_precedence(None, All, Distinct)

A, C, E, F, P, TA, Q, QS = Alias, Condition, Expr, Field, Placeholder, TableAlias, Query, Query
func = const = ConstantSpace()
qn = lambda name, compile: compile(Name(name))[0]

for cls in (Expr, Table, TableJoin, Modify, CompositeExpr, EscapeForLike, Name, Value):
    cls.__repr__ = lambda self: "<{0}: {1}, {2!r}>".format(type(self).__name__, *compile(self))
