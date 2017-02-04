from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.constants import PLACEHOLDER
from sqlbuilder.smartsql.utils import is_list

__all__ = ('Operable', 'Expr', 'expr_repr', 'datatypeof', )


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


def datatypeof(obj):
    if isinstance(obj, Operable):
        return obj._datatype

    from sqlbuilder.smartsql.datatypes import BaseType
    return BaseType


def expr_repr(expr):
    return "<{0}: {1}, {2!r}>".format(type(expr).__name__, *compile(expr))
