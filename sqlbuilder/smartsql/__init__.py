# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/
# But the code fully another... It's not a fork anymore...
from __future__ import absolute_import
import sys
import copy
import types
import operator
import warnings
from functools import wraps, reduce, partial
from weakref import WeakKeyDictionary

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


class ClassRegistry(object):
    """Minimalistic factory for related classes.

    Allows use extended subclasses, if need.
    """
    def __call__(self, name_or_cls):
        name = name_or_cls if isinstance(name_or_cls, string_types) else name_or_cls.__name__

        def deco(cls):
            setattr(self, name, cls)
            if not getattr(cls, '_cr', None) is self:  # save mem
                cls._cr = self
            return cls

        return deco if isinstance(name_or_cls, string_types) else deco(name_or_cls)


cr = ClassRegistry()


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
        self._children = WeakKeyDictionary()
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
        if state.callers:
            if state.callers[0] in (OmitParentheses, Parentheses):
                parentheses = False
            elif isinstance(expr, Query) or type(expr) == Expr:
                parentheses = True

        outer_precedence = state.precedence
        if hasattr(expr, '_sql') and (cls, expr._sql) in self._precedence:
            inner_precedence = self._precedence[(cls, expr._sql)]
        elif hasattr(expr, '_sql') and expr._sql in self._precedence:
            inner_precedence = self._precedence[expr._sql]
        elif cls in self._precedence:
            inner_precedence = self._precedence[cls]
        else:
            inner_precedence = MAX_PRECEDENCE  # self._precedence.get('(any other)', MAX_PRECEDENCE)

        state.precedence = inner_precedence
        if inner_precedence < outer_precedence and parentheses is None:
            parentheses = True

        state.callers.insert(0, expr.__class__)

        if parentheses:
            state.sql.append('(')

        for c in cls.mro():
            if c in self._registry:
                self._registry[c](self, expr, state)
                break
        else:
            raise Error("Unknown compiler for {}".format(cls))

        if parentheses:
            state.sql.append(')')
        state.callers.pop(0)
        state.precedence = outer_precedence


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


def same(name):
    def f(self, *a, **kw):
        return getattr(self, name)(*a, **kw)
    return f


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
        return lambda other: Condition(self, op, other)

    def rop(self, op):  # useless, can be P('lookingfor').op('=')(expr)
        return lambda other: Condition(other, op, self)

    def asc(self):
        return Asc(self)

    def desc(self):
        return Desc(self)

    def __getitem__(self, key):
        """Returns self.between()"""
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return Between(self, start, end)
        else:
            return self.__eq__(key)

    # __hash__ = None


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


class Condition(Expr):
    __slots__ = ('_left', '_right')

    def __init__(self, left, op, right):
        self._left = left
        self._sql = op.upper()
        self._right = right


@compile.when(Condition)
def compile_condition(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(SPACE)
    state.sql.append(expr._sql)
    state.sql.append(SPACE)
    compile(expr._right, state)


class NamedCondition(Condition):
    __slots__ = ()

    def __init__(self, left, right):
        self._left = left
        self._right = right


class Add(NamedCondition):
    _sql = '+'


class Sub(NamedCondition):
    __slots__ = ()
    _sql = '-'


class Mul(NamedCondition):
    __slots__ = ()
    _sql = '*'


class Div(NamedCondition):
    __slots__ = ()
    _sql = '/'


class Gt(NamedCondition):
    __slots__ = ()
    _sql = '>'


class Lt(NamedCondition):
    __slots__ = ()
    _sql = '<'


class Ge(NamedCondition):
    __slots__ = ()
    _sql = '>='


class Le(NamedCondition):
    __slots__ = ()
    _sql = '<='


class And(NamedCondition):
    __slots__ = ()
    _sql = 'AND'


class Or(NamedCondition):
    __slots__ = ()
    _sql = 'OR'


class Eq(NamedCondition):
    __slots__ = ()
    _sql = '='


class Ne(NamedCondition):
    __slots__ = ()
    _sql = '<>'


class Is(NamedCondition):
    __slots__ = ()
    _sql = 'IS'


class IsNot(NamedCondition):
    __slots__ = ()
    _sql = 'IS NOT'


class In(NamedCondition):
    __slots__ = ()
    _sql = 'IN'


class NotIn(NamedCondition):
    __slots__ = ()
    _sql = 'NOT IN'


class RShift(NamedCondition):
    __slots__ = ()
    _sql = ">>"


class LShift(NamedCondition):
    __slots__ = ()
    _sql = "<<"


class EscapeForLike(Expr):

    __slots__ = ('_expr')

    _escape = "!"
    _escape_map = tuple(  # Ordering is important!
        (i, "!{}".format(i)) for i in ('!', '_', '%')
    )

    def __init__(self, expr):
        self._expr = expr


@compile.when(EscapeForLike)
def compile_escapeforlike(compile, expr, state):
    escaped = expr._expr
    for k, v in expr._escape_map:
        escaped = Replace(escaped, Value(k), Value(v))
    compile(escaped, state)


class Like(NamedCondition):
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
    state.sql.append('(')
    compile(expr._expr, state)
    state.sql.append(')')


class OmitParentheses(Parentheses):
    pass


@compile.when(OmitParentheses)
def compile_omitparentheses(compile, expr, state):
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
    cases = [
        "WHEN %s THEN %s" % (
            compile(condition, state), compile(value, state))
        for condition, value in expr._cases]

    if expr._expr is not Undef:
        expression = compile(expr._expr, state) + " "
    else:
        expression = ""

    if expr._default is not Undef:
        default = " ELSE %s" % compile(expr._default, state)
    else:
        default = ""

    state.sql.append("CASE %s%s%s END" % (expression, " ".join(cases), default))


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
        if key[0] == '_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 2)
        prefix, name, alias = parts + [None] * (3 - len(parts))
        if name is None:
            prefix, name = name, prefix
        f = cls(name, prefix)
        return f.as_(alias) if alias else f


class Field(MetaField("NewBase", (Expr,), {})):

    __slots__ = ('_name', '_prefix', '__cached__')

    def __init__(self, name, prefix=None):
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
    if expr._name == '*':
        state.sql.append(expr._name)
    else:
        compile(Name(expr._name), state)


class Alias(Expr):

    __slots__ = ('_expr', '_sql')

    def __init__(self, alias, expr=None):
        self._expr = expr
        super(Alias, self).__init__(alias)


@compile.when(Alias)
def compile_alias(compile, expr, state):
    try:
        render_column = state.callers[1] == FieldList
        # render_column = state.context == CONTEXT_COLUMN
    except IndexError:
        pass
    else:
        if render_column:
            compile(expr._expr, state)
            state.sql.append(' AS ')
    compile(Name(expr._sql), state)


class MetaTable(type):

    def __new__(cls, name, bases, attrs):
        if bases[0] is object:
            def _f(attr):
                return lambda self, *a, **kw: getattr(self._cr.TableJoin(self), attr)(*a, **kw)

            # FIXME: Bad idea, we need free name space for fields.
            # Use one of this form?
            # 1. T.book.get_field('hint')
            # 2. F('hint', T.book)
            # 3. T.fields.hint
            for a in ['inner_join', 'left_join', 'right_join', 'full_join', 'cross_join',
                      'join', 'on', 'hint', 'natural', 'using']:
                attrs[a] = _f(a)
        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        table = cls(name)
        return table.as_(alias) if alias else table


class FieldProxy(object):

    def __init__(self, table):
        self.__table = table

    def __getattr__(self, key):
        return self.__table.__getattr__(key)


@cr
class Table(MetaTable("NewBase", (object, ), {})):

    __slots__ = ('_name', '__cached__', 'fields')

    def __init__(self, name):
        self._name = name
        self.__cached__ = {}
        self.fields = FieldProxy(self)

    def as_(self, alias):
        return self._cr.TableAlias(alias, self)

    def __getattr__(self, key):
        if key[0] == '_':
            raise AttributeError

        if key in self.fields.__dict__:
            return self.fields.__dict__[key]

        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))

        if name in self.fields.__dict__:
            f = self.fields.__dict__[name]
        else:
            f = Field(name, self)
            setattr(self.fields, name, f)
        if alias:
            f = f.as_(alias)
            setattr(self.fields, key, f)
        return f

    __and__ = same('inner_join')
    __add__ = same('left_join')
    __sub__ = same('right_join')
    __or__ = same('full_join')
    __mul__ = same('cross_join')
    get_field = same('__getattr__')


@compile.when(Table)
def compile_table(compile, expr, state):
    compile(Name(expr._name), state)


@cr
class TableAlias(Table):

    __slots__ = ('_table', '_alias', 'fields')

    def __init__(self, alias, table=None):
        self._table = table
        self._alias = alias
        self.__cached__ = {}
        self.fields = FieldProxy(self)

    def as_(self, alias):
        return type(self)(alias, self._table)


@compile.when(TableAlias)
def compile_tablealias(compile, expr, state):
    # if expr._table is not None and state.context == CONTEXT_TABLE:
    try:
        render_table = expr._table is not None and state.callers[1] == TableJoin
        # render_table = expr._table is not None and state.context == CONTEXT_TABLE
    except IndexError:
        pass
    else:
        if render_table:
            compile(expr._table, state)
            state.sql.append(' AS ')
    compile(Name(expr._alias), state)


@cr
class TableJoin(object):

    __slots__ = ('_table', '_alias', '_join_type', '_on', '_left', '_hint', '_nested', '_natural', '_using')

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


@cr
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
        self._distinct = False
        self._fields = FieldList().join(", ")
        if tables is not None:
            if not isinstance(tables, TableJoin):
                tables = self._cr.TableJoin(tables)
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
        self._tables = tables if isinstance(tables, TableJoin) else self._cr.TableJoin(tables)
        return self

    def distinct(self, val=None):
        if val is None:
            return self._distinct
        self = self.clone()
        self._distinct = val
        return self

    @opt_checker(["reset", ])
    def fields(self, *args, **opts):
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

    def count(self):
        return self.result(SelectCount(self)).count()

    def insert(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        return self.result(self._cr.Insert(map=key_values, **kw)).insert()

    def insert_many(self, fields, values, **kw):
        # Deprecated
        return self.insert(fields=fields, values=values, **kw)

    def update(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        kw.setdefault('where', self._wheres)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(self._cr.Update(map=key_values, **kw)).update()

    def delete(self, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('where', self._wheres)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(self._cr.Delete(**kw)).delete()

    def as_table(self, alias):
        return self._cr.TableAlias(alias, self)

    def set(self, all=False):
        return self._cr.Set(self, all=all, result=self.result)

    def raw(self, sql, params=()):
        return self._cr.Raw(sql, params, result=self.result)

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

    columns = same('fields')
    __copy__ = same('clone')


QuerySet = Query


@compile.when(Query)
def compile_query(compile, expr, state):
    state.push("auto_tables", [])  # this expr can be a subquery
    state.sql.append("SELECT ")
    if expr._distinct:
        state.sql.append("DISTINCT ")
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


@cr
class SelectCount(Query):

    def __init__(self, q):
        Query.__init__(self, q.order_by(reset=True).as_table('count_list'))
        self._fields.append(Constant('COUNT')(Constant('1')).as_('count_value'))


@cr
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


@cr
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


@cr
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


@cr
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


@cr
class Set(Query):

    def __init__(self, *exprs, **kw):
        super(Set, self).__init__()
        if 'op' in kw:
            self._sql = kw['op']
        self._all = kw.get('all', False)
        self._exprs = ExprList(*exprs)
        if 'result' in kw:
            self.result = kw['result']
        else:
            self.result = self.result.clone()

    def _op(self, cls, other):
        c = self
        if self.__class__ is self._cr.Set:
            c = cls(*self._exprs, all=self._all)
        elif self.__class__ is not cls:
            c = cls(self, all=self._all)
        c._exprs.append(other)
        return c

    def __or__(self, other):
        return self._op(self._cr.Union, other)

    def __and__(self, other):
        return self._op(self._cr.Intersect, other)

    def __sub__(self, other):
        return self._op(self._cr.Except, other)

    def all(self, all=True):
        self._all = all
        return self

    def clone(self, *attrs):
        self = Query.clone(self, *attrs)
        self._exprs = copy.copy(self._exprs)
        return self


@cr
class Union(Set):
    __slots__ = ()
    _sql = 'UNION'


@cr
class Intersect(Set):
    __slots__ = ()
    _sql = 'INTERSECT'


@cr
class Except(Set):
    __slots__ = ()
    _sql = 'EXCEPT'


@compile.when(Set)
def compile_set(compile, expr, state):
    if expr._all:
        op = ' {} ALL '.format(expr._sql)
    else:
        op = ' {} '.format(expr._sql)
    compile(expr._exprs.join(op), state)
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


@compile.when(Name)
def compile_name(compile, expr, state):
    state.sql.append('"')
    state.sql.append(expr._name)
    state.sql.append('"')


class Value(object):

    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(Value)
def compile_value(compile, expr, state):
    state.sql.append("'")
    state.sql.append(str(expr._value).replace('%', '%%').replace("'", "''"))
    state.sql.append("'")


def is_list(v):
    return isinstance(v, (list, tuple))


def warn(old, new, stacklevel=3):
    warnings.warn("{0} is deprecated. Use {1} instead".format(old, new), PendingDeprecationWarning, stacklevel=stacklevel)

compile.set_precedence(230, '.')
compile.set_precedence(220, '::')
compile.set_precedence(210, '[', ']')  # array element selection
compile.set_precedence(200, Pos, Neg, (Unary, '+'), (Unary, '-'))  # unary minus
compile.set_precedence(190, '^')
compile.set_precedence(180, Mul, Div, '*', '/', '%')
compile.set_precedence(170, Add, Sub, (Condition, '+'), (Condition, '-'))
compile.set_precedence(160, Is, 'IS')
compile.set_precedence(150, 'ISNULL')
compile.set_precedence(140, 'NOTNULL')
compile.set_precedence(130, '(any other)')  # all other native and user-defined operators
compile.set_precedence(120, In, NotIn, 'IN')
compile.set_precedence(110, Between, 'BETWEEN')
compile.set_precedence(100, 'OVERLAPS')
compile.set_precedence(90, Like, Ilike, 'LIKE', 'ILIKE', 'SIMILAR')
compile.set_precedence(80, Lt, Gt, '<', '>')
compile.set_precedence(70, Le, Ge, Ne, '<=', '>=', '<>', '!=')
compile.set_precedence(60, Eq, '=')
compile.set_precedence(50, Not, 'NOT')
compile.set_precedence(40, And, 'AND')
compile.set_precedence(30, Or, 'OR')
compile.set_precedence(10, Query, Insert, Update, Delete, Expr)

A, C, E, F, P, T, TA, Q, QS = Alias, Condition, Expr, Field, Placeholder, Table, TableAlias, Query, Query
func = const = ConstantSpace()
qn = lambda name, compile: compile(Name(name))[0]

for cls in (Expr, Table, TableJoin, Modify, CompositeExpr, EscapeForLike, Name, Value):
    cls.__repr__ = lambda self: "<{0}: {1}, {2!r}>".format(type(self).__name__, *compile(self))
