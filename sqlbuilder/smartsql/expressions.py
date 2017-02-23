from __future__ import absolute_import
import sys
import copy
import operator
from functools import reduce
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.constants import CONTEXT, PLACEHOLDER, MAX_PRECEDENCE
from sqlbuilder.smartsql.exceptions import MaxLengthError
from sqlbuilder.smartsql.pycompat import string_types
from sqlbuilder.smartsql.utils import Undef, is_list, warn

__all__ = (
    'Operable', 'Expr', 'ExprList', 'CompositeExpr', 'Param', 'Parentheses', 'OmitParentheses',
    'Callable', 'NamedCallable', 'Constant', 'ConstantSpace', 'Case', 'Cast', 'Concat',
    'Alias', 'Name', 'NameCompiler', 'Value', 'ValueCompiler', 'Array', 'ArrayItem',
    'expr_repr', 'datatypeof', 'const', 'func'
)

SPACE = " "


@compile.when(object)
def compile_object(compile, expr, state):
    state.sql.append(PLACEHOLDER)
    state.params.append(expr)


@compile.when(type(None))
def compile_none(compile, expr, state):
    state.sql.append('NULL')


@compile.when(slice)
def compile_slice(compile, expr, state):
    # FIXME: Should be here numrange()? Looks like not, see http://initd.org/psycopg/docs/extras.html#adapt-range
    state.sql.append("[")
    state.sql.append("{0:d}".format(expr.start))
    if expr.stop is not None:
        state.sql.append(", ")
        state.sql.append("{0:d}".format(expr.stop))
    state.sql.append("]")


@compile.when(list)
@compile.when(tuple)
def compile_list(compile, expr, state):
    compile(Parentheses(ExprList(*expr).join(", ")), state)


class Operable(object):
    __slots__ = ('_datatype', '__weakref__')

    def __init__(self, datatype=None):
        if datatype is None:
            from sqlbuilder.smartsql.datatypes import BaseType
            datatype = BaseType
        self._datatype = datatype

    def __getattr__(self, name):
        """Use in derived classes:

        try:
            return Operable.__getattr__(self, key)
        except AttributeError:
            return derived_logic()
        """
        if name.startswith('__'):  # All allowed special method already defined.
            raise AttributeError
        delegate = self._datatype(self)
        return getattr(delegate, name)

    __hash__ = object.__hash__

    def __add__(self, other):
        return self._datatype(self).__add__(other)

    def __radd__(self, other):
        return self._datatype(self).__radd__(other)

    def __sub__(self, other):
        return self._datatype(self).__sub__(other)

    def __rsub__(self, other):
        return self._datatype(self).__rsub__(other)

    def __mul__(self, other):
        return self._datatype(self).__mul__(other)

    def __rmul__(self, other):
        return self._datatype(self).__rmul__(other)

    def __div__(self, other):
        return self._datatype(self).__div__(other)

    def __rdiv__(self, other):
        return self._datatype(self).__rdiv__(other)

    def __truediv__(self, other):
        return self._datatype(self).__truediv__(other)

    def __rtruediv__(self, other):
        return self._datatype(self).__rtruediv__(other)

    def __floordiv__(self, other):
        return self._datatype(self).__floordiv__(other)

    def __rfloordiv__(self, other):
        return self._datatype(self).__rfloordiv__(other)

    def __and__(self, other):
        return self._datatype(self).__and__(other)

    def __rand__(self, other):
        return self._datatype(self).__rand__(other)

    def __or__(self, other):
        return self._datatype(self).__or__(other)

    def __ror__(self, other):
        return self._datatype(self).__ror__(other)

    def __gt__(self, other):
        return self._datatype(self).__gt__(other)

    def __lt__(self, other):
        return self._datatype(self).__lt__(other)

    def __ge__(self, other):
        return self._datatype(self).__ge__(other)

    def __le__(self, other):
        return self._datatype(self).__le__(other)

    def __eq__(self, other):
        return self._datatype(self).__eq__(other)

    def __ne__(self, other):
        return self._datatype(self).__ne__(other)

    def __rshift__(self, other):
        return self._datatype(self).__rshift__(other)

    def __rrshift__(self, other):
        return self._datatype(self).__rshift__(other)

    def __lshift__(self, other):
        return self._datatype(self).__lshift__(other)

    def __rlshift__(self, other):
        return self._datatype(self).__lshift__(other)

    def __pos__(self):
        return self._datatype(self).__pos__()

    def __neg__(self):
        return self._datatype(self).__neg__()

    def __invert__(self):
        return self._datatype(self).__invert__()

    def __pow__(self, other):
        return self._datatype(self).__pow__(other)

    def __rpow__(self, other):
        return self._datatype(self).__rpow__(other)

    def __mod__(self, other):
        return self._datatype(self).__mod__(other)

    def __rmod__(self, other):
        return self._datatype(self).__rmod__(other)

    def __abs__(self):
        return self._datatype(self).__abs__()

    def __getitem__(self, key):
        return self._datatype(self).__getitem__(key)


class Expr(Operable):
    __slots__ = ('sql', 'params')

    def __init__(self, sql, *params, **kwargs):
        Operable.__init__(self, kwargs.get('datatype'))
        if params and is_list(params[0]):
            self.__init__(sql, *params[0])
            return
        self.sql, self.params = sql, params

    def __repr__(self):
        return expr_repr(self)


@compile.when(Expr)
def compile_expr(compile, expr, state):
    state.sql.append(expr.sql)
    state.params += expr.params


class ExprList(Expr):

    __slots__ = ('data', )

    def __init__(self, *args):
        # if args and is_list(args[0]):
        #     self.__init__(*args[0])
        #     return
        Expr.__init__(self, ' ')
        self.data = list(args)

    def join(self, sep):
        self.sql = sep
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

    def extend(self, l):
        return self.data.extend(l)

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
            state.sql.append(expr.sql)
        compile(a, state)


class CompositeExpr(object):

    __slots__ = ('data', 'sql')

    def __init__(self, *args):
        self.data = args
        self.sql = ", "

    def as_(self, aliases):
        return self.__class__(*(expr.as_(alias) for expr, alias in zip(self.data, aliases)))

    def in_(self, composite_others):
        return self._op_list(operator.eq, composite_others)

    def not_in(self, composite_others):
        return ~self._op_list(operator.eq, composite_others)

    def _op_list(self, op, composite_others):
        return reduce(operator.or_, (self._op(op, composite_other) for composite_other in composite_others))

    def _op(self, op, composite_other):
        return reduce(operator.and_, (op(expr, val) for (expr, val) in zip(self.data, composite_other)))

    def __eq__(self, composite_other):
        return self._op(operator.eq, composite_other)

    def __ne__(self, composite_other):
        return self._op(operator.ne, composite_other)

    def __iter__(self):
        return iter(self.data)

    def __repr__(self):
        return expr_repr(self)


@compile.when(CompositeExpr)
def compile_compositeexpr(compile, expr, state):
    state.push('callers')
    state.callers.pop(0)  # pop CompositeExpr from caller's stack to correct render of aliases.
    compile_exprlist(compile, expr, state)
    state.pop()


class Param(Expr):

    __slots__ = ()

    def __init__(self, params):
        Operable.__init__(self)
        self.params = params


@compile.when(Param)
def compile_param(compile, expr, state):
    compile(expr.params, state)


class Parentheses(Expr):

    __slots__ = ('expr', )

    def __init__(self, expr):
        Operable.__init__(self)
        self.expr = expr


@compile.when(Parentheses)
def compile_parentheses(compile, expr, state):
    state.precedence += MAX_PRECEDENCE
    compile(expr.expr, state)


class OmitParentheses(Parentheses):
    pass


@compile.when(OmitParentheses)
def compile_omitparentheses(compile, expr, state):
    state.precedence = 0
    compile(expr.expr, state)


class Callable(Expr):

    __slots__ = ('expr', 'args')

    def __init__(self, expr, *args):
        Operable.__init__(self)
        self.expr = expr
        self.args = ExprList(*args).join(", ")


@compile.when(Callable)
def compile_callable(compile, expr, state):
    compile(expr.expr, state)
    state.sql.append('(')
    compile(expr.args, state)
    state.sql.append(')')


class NamedCallable(Callable):
    __slots__ = ()

    def __init__(self, *args):
        Operable.__init__(self)
        self.args = ExprList(*args).join(", ")


@compile.when(NamedCallable)
def compile_namedcallable(compile, expr, state):
    state.sql.append(expr.sql)
    state.sql.append('(')
    compile(expr.args, state)
    state.sql.append(')')


class Constant(Expr):

    __slots__ = ()

    def __init__(self, const):
        Expr.__init__(self, const.upper())

    def __call__(self, *args):
        return Callable(self, *args)


@compile.when(Constant)
def compile_constant(compile, expr, state):
    state.sql.append(expr.sql)


class ConstantSpace(object):

    __slots__ = ()

    def __getattr__(self, attr):
        return Constant(attr)


class Case(Expr):
    __slots__ = ('cases', 'expr', 'default')

    def __init__(self, cases, expr=Undef, default=Undef):
        Operable.__init__(self)
        self.cases = cases
        self.expr = expr
        self.default = default


@compile.when(Case)
def compile_case(compile, expr, state):
    state.sql.append('CASE')
    if expr.expr is not Undef:
        state.sql.append(SPACE)
        compile(expr.expr, state)
    for clause, value in expr.cases:
        state.sql.append(' WHEN ')
        compile(clause, state)
        state.sql.append(' THEN ')
        compile(value, state)
    if expr.default is not Undef:
        state.sql.append(' ELSE ')
        compile(expr.default, state)
    state.sql.append(' END ')


class Cast(NamedCallable):
    __slots__ = ("expr", "type",)
    sql = "CAST"

    def __init__(self, expr, type):
        Operable.__init__(self)
        self.expr = expr
        self.type = type


@compile.when(Cast)
def compile_cast(compile, expr, state):
    state.sql.append(expr.sql)
    state.sql.append('(')
    compile(expr.expr, state)
    state.sql.append(' AS ')
    state.sql.append(expr.type)
    state.sql.append(')')


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


class Alias(Expr):

    __slots__ = ('expr', 'sql')

    def __init__(self, expr=Undef, name=Undef):
        if isinstance(expr, string_types):
            warn('Alias(alias, expr)', 'Alias(name, expr)')
            expr, name = name, expr
        self.expr = expr
        if isinstance(name, string_types):
            name = Name(name)
        super(Alias, self).__init__(name)


@compile.when(Alias)
def compile_alias(compile, expr, state):
    if state.context == CONTEXT.FIELD:
        compile(expr.expr, state)
        state.sql.append(' AS ')
    compile(expr.sql, state)


class Name(object):

    __slots__ = ('name', )

    def __init__(self, name=None):
        self.name = name

    def __repr__(self):
        return expr_repr(self)


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
    _delimiter = '"'
    _escape_delimiter = '"'
    _max_length = 63

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_{}'.format(k), v)

    def __call__(self, compile, expr, state):
        state.sql.append(self._delimiter)
        name = expr.name
        name = name.replace(self._delimiter, self._escape_delimiter + self._delimiter)
        for k, v in self._translation_map:
            name = name.replace(k, v)
        if len(name) > self._get_max_length(state):
            raise MaxLengthError("The length of name {0!r} is more than {1}".format(name, self._max_length))
        state.sql.append(name)
        state.sql.append(self._delimiter)

    def _get_max_length(self, state):
        # Max length can depend on context.
        return self._max_length

compile_name = NameCompiler()
compile.when(Name)(compile_name)


class Value(object):

    __slots__ = ('value', )

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return expr_repr(self)


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
    _delimiter = "'"
    _escape_delimiter = "'"

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_{}'.format(k), v)

    def __call__(self, compile, expr, state):
        state.sql.append(self._delimiter)
        value = str(expr.value)
        value = value.replace(self._delimiter, self._escape_delimiter + self._delimiter)
        for k, v in self._translation_map:
            value = value.replace(k, v)
        state.sql.append(value)
        state.sql.append(self._delimiter)


compile_value = ValueCompiler()
compile.when(Value)(compile_value)


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


class ArrayItem(Expr):

    __slots__ = ('array', 'key')

    def __init__(self, array, key):
        Operable.__init__(self)
        self.array = array
        assert isinstance(key, slice)
        self.key = key


@compile.when(ArrayItem)
def compile_arrayitem(compile, expr, state):
    compile(expr.array)
    state.sql.append("[")
    state.sql.append("{0:d}".format(expr.key.start))
    if expr.key.stop is not None:
        state.sql.append(", ")
        state.sql.append("{0:d}".format(expr.key.stop))
    state.sql.append("]")


def datatypeof(obj):
    if isinstance(obj, Operable):
        return obj._datatype

    from sqlbuilder.smartsql.datatypes import BaseType
    return BaseType


def expr_repr(expr):
    return "<{0}: {1}, {2!r}>".format(type(expr).__name__, *compile(expr))

func = const = ConstantSpace()
