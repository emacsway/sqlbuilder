from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.constants import PLACEHOLDER

__all__ = ('expr_repr', )


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


def expr_repr(expr):
    return "<{0}: {1}, {2!r}>".format(type(expr).__name__, *compile(expr))
