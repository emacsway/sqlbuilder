from __future__ import absolute_import
import copy
import collections
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.constants import LOOKUP_SEP, CONTEXT
from sqlbuilder.smartsql.expressions import CompositeExpr, Expr, ExprList, OmitParentheses, Name, expr_repr
from sqlbuilder.smartsql.factory import factory
from sqlbuilder.smartsql.fields import Field
from sqlbuilder.smartsql.pycompat import string_types
from sqlbuilder.smartsql.utils import same, warn

__all__ = (
    'MetaTableSpace', 'T', 'MetaTable', 'FieldProxy', 'Table', 'TableAlias', 'TableJoin',
    'Join', 'InnerJoin', 'LeftJoin', 'RightJoin', 'FullJoin', 'CrossJoin', 'ModelRegistry', 'model_registry',
)

SPACE = " "


@factory.register
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
        table = cls(name)
        return table.as_(alias) if alias else table

    def __call__(cls, name, *a, **kw):
        return cls.__factory__.Table(name, *a, **kw)


@factory.register
class T(factory.MetaTableSpace("NewBase", (object, ), {})):
    pass


class MetaTable(type):

    def __getattr__(cls, key):
        if key[:2] == '__':
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

    def __repr__(self):
        return "<{0}: {1}>".format(type(self).__name__, expr_repr(self.id._prefix))


@compile.when(FieldProxy)
def compile_fieldproxy(compile, expr, state):
    compile(expr.id._prefix, state)


# TODO: Schema support. Not only for table.
# A database contains one or more named schemas, which in turn contain tables.
# Schemas also contain other kinds of named objects, including data types, functions, and operators.
# http://www.postgresql.org/docs/9.4/static/ddl-schemas.html
# Ideas: S.public(T.user), S('public', T.user)


@factory.register
class Table(MetaTable("NewBase", (object, ), {})):
    # Variants:
    # tb.as_ => Field(); tb().as_ => instancemethod() ???
    # author + InnerJoin + book + On + author.id == book.author_id
    # Add __call__() method to Field/Alias
    # Use sys._getframe(), compiler.visitor.ASTVisitor or tokenize.generate_tokens() to get context for Table.__getattr__()

    __slots__ = ('_name', '__cached__', 'f', '_fields', '__factory__')

    def __init__(self, name, *fields):
        if isinstance(name, string_types):
            name = Name(name)
        self._name = name
        self.__cached__ = {}
        self.f = FieldProxy(self)
        self._fields = collections.OrderedDict()
        for f in fields:
            self._append_field(f)

    def as_(self, alias):
        return factory.get(self).TableAlias(self, alias)

    def inner_join(self, right):
        return factory.get(self).TableJoin(self).inner_join(right)

    def left_join(self, right):
        return factory.get(self).TableJoin(self).left_join(right)

    def right_join(self, right):
        return factory.get(self).TableJoin(self).right_join(right)

    def full_join(self, right):
        return factory.get(self).TableJoin(self).full_join(right)

    def cross_join(self, right):
        return factory.get(self).TableJoin(self).cross_join(right)

    def join(self, join_type, obj):
        return factory.get(self).TableJoin(self).join(join_type, obj)

    def on(self, cond):
        return factory.get(self).TableJoin(self).on(cond)

    def hint(self, expr):
        return factory.get(self).TableJoin(self).hint(expr)

    def natural(self):
        return factory.get(self).TableJoin(self).natural()

    def using(self, *fields):
        return factory.get(self).TableJoin(self).using(*fields)

    def _append_field(self, field):
        self._fields[field._name] = field
        field.prefix = self

    def get_field(self, key):
        cache = self.f.__dict__
        if key in cache:
            return cache[key]

        if type(key) == tuple:
            cache[key] = CompositeExpr(*(self.get_field(k) for k in key))
            return cache[key]

        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))

        if name in cache:
            f = cache[name]
        else:
            f = self._fields[name] if name in self._fields else Field(name, self)
            cache[name] = f
        if alias:
            f = f.as_(alias)
            cache[key] = f
        return f

    def __getattr__(self, key):
        if key[:2] == '__' or key in Table.__slots__:
            raise AttributeError
        return self.get_field(key)

    def __getitem__(self, key):
        return self.get_field(key)

    def __repr__(self):
        return expr_repr(self)

    __and__ = same('inner_join')
    __add__ = same('left_join')
    __sub__ = same('right_join')
    __or__ = same('full_join')
    __mul__ = same('cross_join')


@compile.when(Table)
def compile_table(compile, expr, state):
    compile(expr._name, state)


@factory.register
class TableAlias(Table):

    __slots__ = ('_table',)

    def __init__(self, table, name, *fields):
        if isinstance(table, string_types):
            warn('TableAlias(name, table, *fields)', 'TableAlias(table, name, *fields)')
            table, name = name, table
        Table.__init__(self, name, *fields)
        self._table = table
        if not fields and isinstance(table, Table):
            for f in table._fields.values():
                self._append_field(copy.copy(f))

    def as_(self, alias):
        return type(self)(alias, self._table)


@compile.when(TableAlias)
def compile_tablealias(compile, expr, state):
    if expr._table is not None and state.context == CONTEXT.TABLE:
        compile(expr._table, state)
        state.sql.append(' AS ')
    compile(expr._name, state)


@factory.register
class TableJoin(object):

    __slots__ = ('_table', '_join_type', '_on', '_left', '_hint', '_nested', '_natural', '_using', '__factory__')

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

    def inner_join(self, right):
        return self.join("INNER JOIN", right)

    def left_join(self, right):
        return self.join("LEFT OUTER JOIN", right)

    def right_join(self, right):
        return self.join("RIGHT OUTER JOIN", right)

    def full_join(self, right):
        return self.join("FULL OUTER JOIN", right)

    def cross_join(self, right):
        return self.join("CROSS JOIN", right)

    def join(self, join_type, right):
        if not isinstance(right, TableJoin) or right.left():
            right = type(self)(right, left=self)
        else:
            right = copy.copy(right)
        right = right.left(self).join_type(join_type)
        return right

    def left(self, left=None):
        if left is None:
            return self._left
        self._left = left
        return self

    def join_type(self, join_type):
        self._join_type = join_type
        return self

    def on(self, cond):
        if self._on is not None:
            c = self.__class__(self)  # TODO: Test me.
        else:
            c = self
        c._on = cond
        return c

    def natural(self):
        self._natural = True
        return self

    def using(self, *fields):
        self._using = ExprList(*fields).join(", ")
        return self

    def __call__(self):
        self._nested = True
        c = self.__class__(self)
        return c

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

    def __repr__(self):
        return expr_repr(self)

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
    state.push('context', CONTEXT.TABLE)
    compile(expr._table, state)
    state.pop()
    if expr._on is not None:
        state.sql.append(' ON ')
        state.push("context", CONTEXT.EXPR)
        compile(expr._on, state)
        state.pop()
    elif expr._using is not None:
        state.sql.append(' USING ')
        compile(expr._using, state)
    if expr._hint is not None:
        state.sql.append(SPACE)
        compile(expr._hint, state)
    if expr._nested:
        state.sql.append(')')


# Model based table

class NamedJoin(TableJoin):
    __slots__ = ()

    def __init__(self, left, right, on=None):
        self._table = right
        self._on = on
        self._left = left
        self._hint = None
        self._nested = False
        self._natural = False
        self._using = None


class Join(NamedJoin):
    __slots__ = ()
    _join_type = "JOIN"


class InnerJoin(NamedJoin):
    __slots__ = ()
    _join_type = "INNER JOIN"


class LeftJoin(NamedJoin):
    __slots__ = ()
    _join_type = "LEFT OUTER JOIN"


class RightJoin(NamedJoin):
    __slots__ = ()
    _join_type = "RIGHT OUTER JOIN"


class FullJoin(NamedJoin):
    __slots__ = ()
    _join_type = "FULL OUTER JOIN"


class CrossJoin(NamedJoin):
    __slots__ = ()
    _join_type = "CROSS JOIN"


class ModelRegistry(dict):
    def __setitem__(self, key, value):
        super(ModelRegistry, self).__setitem__(key, Name(value))

    def register(self, table_name):
        def _inner(cls):
            self[cls] = table_name
            return cls
        return _inner


@compile.when(type)
def compile_type(compile, model, state):
    """ Any class can be used as Table """
    compile(model_registry[model], state)


model_registry = ModelRegistry()
