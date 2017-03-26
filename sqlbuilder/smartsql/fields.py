from __future__ import absolute_import
from sqlbuilder.smartsql.compiler import compile, cached_compile
from sqlbuilder.smartsql.constants import LOOKUP_SEP, CONTEXT
from sqlbuilder.smartsql.expressions import Operable, Expr, Constant, ExprList, Parentheses, Name, compile_exprlist
from sqlbuilder.smartsql.pycompat import string_types

__all__ = ('MetaFieldSpace', 'F', 'MetaField', 'Field', 'Subfield', 'FieldList', )


class MetaFieldSpace(type):

    def __instancecheck__(cls, instance):
        return isinstance(instance, Field)

    def __subclasscheck__(cls, subclass):
        return issubclass(subclass, Field)

    def __getattr__(cls, key):
        if key[:2] == '__':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 2)
        prefix, name, alias = parts + [None] * (3 - len(parts))
        if name is None:
            prefix, name = name, prefix
        f = cls(name, prefix)
        return f.as_(alias) if alias else f

    def __call__(cls, *a, **kw):
        return Field(*a, **kw)


class F(MetaFieldSpace("NewBase", (object, ), {})):
    pass


class MetaField(type):

    def __getattr__(cls, key):
        if key[:2] == '__':
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
    __slots__ = ('_name', '_prefix', '__cached__')  # TODO: m_* prefix instead of _* prefix?

    def __init__(self, name, prefix=None, datatype=None):
        Operable.__init__(self, datatype)
        if isinstance(name, string_types):
            if name == '*':
                name = Constant(name)
            else:
                name = Name(name)
        self._name = name
        if isinstance(prefix, string_types):
            from sqlbuilder.smartsql.tables import Table
            prefix = Table(prefix)
        self._prefix = prefix
        self.__cached__ = {}


@compile.when(Field)
@cached_compile  # The cache depends on state.context
def compile_field(compile, expr, state):
    if expr._prefix is not None and state.context != CONTEXT.FIELD_NAME:
        state.auto_tables.append(expr._prefix)  # it's important to know the concrete alias of table.
        state.push("context", CONTEXT.FIELD_PREFIX)
        compile(expr._prefix, state)
        state.pop()
        state.sql.append('.')
    compile(expr._name, state)


class Subfield(Expr):

    __slots__ = ('parent', 'name')

    def __init__(self, parent, name):
        Operable.__init__(self)
        self.parent = parent
        if isinstance(name, string_types):
            name = Name(name)
        self.name = name


@compile.when(Subfield)
def compile_subfield(compile, expr, state):
    parent = expr.parent
    if True:  # get me from context
        parent = Parentheses(parent)
    compile(parent)
    state.sql.append('.')
    compile(expr.name, state)


class FieldList(ExprList):
    __slots__ = ()

    def __init__(self, *args):
        # if args and is_list(args[0]):
        #     return self.__init__(*args[0])
        Operable.__init__(self)
        self.sql, self.data = ", ", list(args)
