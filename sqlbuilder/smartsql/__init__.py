# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/
# But the code fully another... It's not a fork anymore...
from __future__ import absolute_import, unicode_literals
import sys
import copy
import types
import warnings
from functools import wraps
from weakref import WeakKeyDictionary

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

    def i(v):
        return v.encode('utf-8')

except NameError:
    string_types = (str,)
    integer_types = (int,)

    def i(v):
        return v

DEFAULT_DIALECT = 'postgres'
PLACEHOLDER = "%s"  # Can be re-defined by registered dialect.
LOOKUP_SEP = i('__')
MAX_PRECEDENCE = 1000
SPACE = " "

CONTEXT_QUERY = 0
CONTEXT_COLUMN = 1
CONTEXT_TABLE = 2


class State(object):

    def __init__(self):
        self.sql = []
        self.params = []
        self._stack = []
        self._callers = []
        self.context = CONTEXT_QUERY
        self.precedence = 0

    def push(self, attr, new_value):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is None:
            new_value = copy(old_value)
        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        setattr(self, *self._stack.pop(-1))


class Compiler(object):

    def __init__(self, parent=None):
        self._children = WeakKeyDictionary()
        if parent:
            self._parents = []
            self._parents.extend(parent._parents)
            self._parents.append()
            parent._children[self] = True
        self._local_registry = {}
        self._local_precedence = {}
        self._registry = {}
        self._precedence = {}

    def create_child(self):
        return self.__class__(self)

    def register(self, cls):
        def deco(func):
            self._local_registry[cls] = func
            self._update_cache()
            return func
        return deco

    def _update_cache(self):
        self._registry.update(self._local_registry)
        self._precedence.update(self._local_precedence)
        for child in self._children:
            child._update_cache()

    def sqlrepr(self, expr):
        state = State()
        self(expr, state)
        return ''.join(state.sql), state.params

    def __call__(self, expr, state):
        cls = expr.__class__
        parentheses = False
        if state._callers:
            if isinstance(expr, (Condition, QuerySet)) or type(expr) == Expr:
                parentheses = True

        # outer_precedence = state.precedence
        # if hasattr(cls, '_sql') and cls._sql in self._precedence:
        #     inner_precedence = state.precedence = self._precedence[cls._sql]
        # else:
        #     inner_precedence = state.precedence = self._precedence.get(cls, MAX_PRECEDENCE)
        # if inner_precedence < outer_precedence:
        #     parentheses = True

        state._callers.insert(0, expr.__class__)

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
        state._callers.pop(0)
        # state.precedence = outer_precedence


compile = Compiler()


@compile.register(object)
def compile_object(compile, expr, state):
    state.sql.append('%s')
    state.params.append(expr)


@compile.register(types.NoneType)
def compile_none(compile, expr, state):
    state.sql.append('NULL')


@compile.register(list)
@compile.register(tuple)
def compile_list(compile, expr, state):
    compile(Parentheses(ExprList(*expr).join(", ")), state)


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


def same(name):
    def f(self, *a, **kw):
        return getattr(self, name)(*a, **kw)
    return f


class Error(Exception):
    pass


class Comparable(object):

    __slots__ = ()

    def _c(op, inv=False):
        return (lambda self, other: Condition(self, op, other)) if not inv else (lambda self, other: Condition(other, op, self))

    def _ca(op, inv=False):
        return (lambda self, *a: Constant(op)(self, *a)) if not inv else (lambda self, other: Constant(op)(other, self))

    def _p(op):
        return lambda self: Prefix(op, self)

    def _l(mask, ci=False, inv=False):
        a = 'like'
        if ci:
            a = 'i' + a
        if inv:
            a = 'r' + a

        def f(self, other):
            args = [other]
            if 4 & mask:
                args.insert(0, '%')
            if 1 & mask:
                args.append('%')
            return getattr(self, a)(Concat(*args))
        return f

    __add__ = _c("+")
    __radd__ = _c("+", 1)
    __sub__ = _c("-")
    __rsub__ = _c("-", 1)
    __mul__ = _c("*")
    __rmul__ = _c("*", 1)
    __div__ = _c("/")
    __rdiv__ = _c("/", 1)
    __and__ = _c("AND")
    __rand__ = _c("AND", 1)
    __or__ = _c("OR")
    __ror__ = _c("OR", 1)
    __gt__ = _c(">")
    __lt__ = _c("<")
    __ge__ = _c(">=")
    __le__ = _c("<=")
    is_ = _c("IS")
    is_not = _c("IS NOT")
    in_ = _c("IN")
    not_in = _c("NOT IN")
    like = _c("LIKE")
    ilike = _c("ILIKE")
    rlike = _c("LIKE", 1)
    rilike = _c("ILIKE", 1)

    __pos__ = _p("+")
    __neg__ = _p("-")
    __invert__ = _p("NOT")
    distinct = _p("DISTINCT")

    __pow__ = _ca("POW")
    __rpow__ = _ca("POW", 1)
    __mod__ = _ca("MOD")
    __rmod__ = _ca("MOD", 1)
    __abs__ = _ca("ABS")
    count = _ca("COUNT")

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

    def __eq__(self, other):
        if other is None:
            return self.is_(None)
        if is_list(other):
            return self.in_(other)
        return Condition(self, "=", other)

    def __ne__(self, other):
        if other is None:
            return self.is_not(None)
        if is_list(other):
            return self.not_in(other)
        return Condition(self, "<>", other)

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
        return Postfix(self, "ASC")

    def desc(self):
        return Postfix(self, "DESC")

    def __getitem__(self, key):
        """Returns self.between()"""
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return Between(self, start, end)
        else:
            return self.__eq__(key)

    __hash__ = None


class Expr(Comparable):

    __slots__ = (i('_sql'), i('_params'))

    def __init__(self, sql, *params):
        if params and is_list(params[0]):
            return self.__init__(sql, *params[0])
        self._sql, self._params = sql, params


@compile.register(Expr)
def compile_expr(compile, expr, state):
    state.sql.append(expr._sql)
    state.params += expr._params


class Condition(Expr):

    __slots__ = (i('_left'), i('_right'))

    def __init__(self, left, op, right):
        self._left = left
        self._sql = op.upper()
        self._right = right


@compile.register(Condition)
def compile_condition(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(SPACE)
    state.sql.append(expr._sql)
    state.sql.append(SPACE)
    compile(expr._right, state)


class ExprList(Expr):

    __slots__ = (i('_args'), )

    def __init__(self, *args):
        if args and is_list(args[0]):
            return self.__init__(*args[0])
        self._sql, self._args = " ", list(args)

    def join(self, sep):
        self._sql = sep
        return self

    def __len__(self):
        return len(self._args)

    def __setitem__(self, key, value):
        self._args[key] = value

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return ExprList(*self._args[start:end])
        return self._args[key]

    def __iter__(self):
        return iter(self._args)

    def append(self, x):
        return self._args.append(x)

    def insert(self, i, x):
        return self._args.insert(i, x)

    def extend(self, L):
        return self._args.extend(L)

    def pop(self, i):
        return self._args.pop(i)

    def remove(self, x):
        return self._args.remove(x)

    def reset(self):
        self._args = []
        return self

    def __copy__(self):
        dup = copy.copy(super(ExprList, self))
        dup._args = dup._args[:]
        return dup


@compile.register(ExprList)
def compile_exprlist(compile, expr, state):
    first = True
    for a in expr._args:
        if first:
            first = False
        else:
            state.sql.append(expr._sql)
        compile(a, state)


class FieldList(ExprList):
    __slots__ = ()


@compile.register(FieldList)
def compile_fieldlist(compile, expr, state):
    state.push('context', CONTEXT_COLUMN)
    compile_exprlist(compile, expr, state)
    state.pop()


class Concat(ExprList):

    __slots__ = (i('_args'), i('_ws'))

    def __init__(self, *args):
        super(Concat, self).__init__(*args)
        self._sql = ' || '
        self._ws = None

    def ws(self, sep):
        self._ws = sep
        self._sql = ', '
        return self


@compile.register(Concat)
def compile_concat(compile, expr, state):
    if not expr._ws:
        return compile_exprlist(compile, expr, state)
    state.sql.append('concat_ws(')
    compile(expr._ws, state)
    for a in expr._args:
        state.sql.append(expr._sql)
        compile(a, state)
    state.sql.append(')')


class Placeholder(Expr):

    __slots__ = ()

    def __init__(self, *params):
        super(Placeholder, self).__init__(PLACEHOLDER, *params)


class Parentheses(Expr):

    __slots__ = (i('_expr', ))

    def __init__(self, expr):
        self._expr = expr


@compile.register(Parentheses)
def compile_parentheses(compile, expr, state):
    state.sql.append('(')
    compile(expr._expr, state)
    state.sql.append(')')


class OmitParentheses(Parentheses):
    pass


@compile.register(OmitParentheses)
def compile_omitparentheses(compile, expr, state):
    compile(expr._expr, state)


class Prefix(Expr):

    __slots__ = (i('_expr', ))

    def __init__(self, prefix, expr):
        self._sql = prefix
        self._expr = expr


@compile.register(Prefix)
def compile_prefix(compile, expr, state):
    state.sql.append(expr._sql)
    state.sql.append(SPACE)
    compile(expr._expr, state)


class Postfix(Expr):

    __slots__ = (i('_expr', ))

    def __init__(self, expr, postfix):
        self._sql = postfix
        self._expr = expr


@compile.register(Postfix)
def compile_postfix(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append(SPACE)
    state.sql.append(expr._sql)


class Between(Expr):

    __slots__ = (i('_expr'), i('_start'), i('_end'))

    def __init__(self, expr, start, end):
        self._expr, self._start, self._end = expr, start, end


@compile.register(Between)
def compile_between(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append(' BETWEEN ')
    compile(expr._start, state)
    state.sql.append(' AND ')
    compile(expr._end, state)


class Callable(Expr):

    __slots__ = (i('_expr'), i('_args'))

    def __init__(self, expr, *args):
        self._expr = expr
        self._args = ExprList(*args).join(", ")


@compile.register(Callable)
def compile_callable(compile, expr, state):
    compile(expr._expr, state)
    state.sql.append('(')
    compile(expr._args, state)
    state.sql.append(')')


class Constant(Expr):

    __slots__ = ()

    def __init__(self, const):
        self._sql = const.upper()

    def __call__(self, *args):
        return Callable(self, *args)


@compile.register(Constant)
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


class Field(MetaField(i("NewBase"), (Expr,), {})):

    __slots__ = (i('_name'), i('_prefix'), i('__cached__'))

    def __init__(self, name, prefix=None):
        self._name = name
        if isinstance(prefix, string_types):
            prefix = Table(prefix)
        self._prefix = prefix
        self.__cached__ = {}


@compile.register(Field)
def compile_field(compile, expr, state):
    if expr._prefix is not None:
        compile(expr._prefix, state)
        state.sql.append('.')
    if expr._name == '*':
        state.sql.append(expr._name)
    else:
        compile(Name(expr._name), state)


class Alias(Expr):

    __slots__ = (i('_expr'), i('_sql'))

    def __init__(self, alias, expr=None):
        self._expr = expr
        super(Alias, self).__init__(alias)


@compile.register(Alias)
def compile_alias(compile, expr, state):
    if state.context == CONTEXT_COLUMN:
        compile(expr._expr, state)
        state.sql.append(' AS ')
    compile(Name(expr._sql), state)


class MetaTable(type):

    def __new__(cls, name, bases, attrs):
        def _f(attr):
            return lambda self, *a, **kw: getattr(self._cr.TableJoin(self), attr)(*a, **kw)

        for a in [i('inner_join'), i('left_join'), i('right_join'), i('full_join'), i('cross_join'), i('join'), i('on'), i('hint')]:
            attrs[a] = _f(a)
        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        table = cls(name)
        return table.as_(alias) if alias else table


class Table(MetaTable(i("NewBase"), (object, ), {})):

    __slots__ = (i('_name'), i('__cached__'))

    def __init__(self, name):
        self._name = name
        self.__cached__ = {}

    def as_(self, alias):
        return self._cr.TableAlias(alias, self)

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
        parts = name.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        f = Field(name, self)
        if alias:
            f = f.as_(alias)
        setattr(self, name, f)
        return f

    __and__ = same(i('inner_join'))
    __add__ = same(i('left_join'))
    __sub__ = same(i('right_join'))
    __or__ = same(i('full_join'))
    __mul__ = same(i('cross_join'))


@compile.register(Table)
def compile_table(compile, expr, state):
    compile(Name(expr._name), state)


class TableAlias(Table):

    __slots__ = (i('_table'), i('_alias'))

    def __init__(self, alias, table=None):
        self._table = table
        self._alias = alias
        self.__cached__ = {}

    def as_(self, alias):
        return type(self)(alias, self._table)


@compile.register(TableAlias)
def compile_tablealias(compile, expr, state):
    if expr._table is not None and state.context == CONTEXT_TABLE:
        compile(expr._table, state)
        state.sql.append(' AS ')
    compile(Name(expr._alias), state)


class TableJoin(object):

    __slots__ = (i('_table'), i('_alias'), i('_join_type'), i('_on'), i('_left'), i('_hint'), )

    def __init__(self, table_or_alias, join_type=None, on=None, left=None):
        if isinstance(table_or_alias, TableAlias):
            self._table = table_or_alias._table
            self._alias = table_or_alias
        else:
            self._table = table_or_alias
            self._alias = None
        self._join_type = join_type
        self._on = on
        self._left = left
        self._hint = None

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
            self = type(self)(self)
        self._on = c
        return self

    def group(self):
        return type(self)(self)

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

    as_nested = same(i('group'))
    __and__ = same(i('inner_join'))
    __add__ = same(i('left_join'))
    __sub__ = same(i('right_join'))
    __or__ = same(i('full_join'))
    __mul__ = same(i('cross_join'))


@compile.register(TableJoin)
def compile_tablejoin(compile, expr, state):
    sql = ExprList().join(" ")
    if expr._left is not None:
        sql.append(expr._left)
    if expr._join_type:
        sql.append(Constant(expr._join_type))
    if isinstance(expr._table, (TableJoin, )):
        sql.append(Parentheses(expr._table))
    else:
        sql.append(expr._table)
    if expr._alias is not None:
        sql.extend([Constant("AS"), expr._alias])
    if expr._on is not None:
        sql.extend([Constant("ON"), expr._on])
    if expr._hint is not None:
        sql.append(expr._hint)
    compile(sql, state)


class QuerySet(Expr):

    _clauses = (
        ('fields', None, i('_fields')),
        ('tables', None, i('_tables')),
        ('from', 'FROM', i('_tables')),
        ('where', 'WHERE', i('_wheres')),
        ('group', 'GROUP BY', i('_group_by')),
        ('having', 'HAVING', i('_havings')),
        ('order', 'ORDER BY', i('_order_by')),
        ('limit', None, i('_limit'))
    )

    def __init__(self, tables=None):

        self._distinct = False
        self._fields = FieldList().join(", ")
        if tables:
            if not isinstance(tables, TableJoin):
                tables = self._cr.TableJoin(tables)
        self._tables = tables
        self._wheres = None
        self._havings = None
        self._group_by = ExprList().join(", ")
        self._order_by = ExprList().join(", ")
        self._limit = None

        self._values = ExprList().join(", ")
        self._key_values = ExprList().join(", ")
        self._ignore = False
        self._on_duplicate_key_update = False
        self._for_update = False

        self._action = "select"
        self.compile = compile

    def clone(self):
        dup = copy.copy(super(QuerySet, self))
        for a in ['_fields', '_tables', '_group_by', '_order_by', '_values', '_key_values', ]:
            setattr(dup, a, copy.copy(getattr(dup, a, None)))
        return dup

    def tables(self, t=None):
        if t is None:
            return self._tables
        self = self.clone()
        self._tables = t if isinstance(t, TableJoin) else self._cr.TableJoin(t)
        return self

    def distinct(self, val=None):
        if val is None:
            return self._distinct
        self = self.clone()
        self._distinct = val
        return self

    @opt_checker([i("reset"), ])
    def fields(self, *args, **opts):
        if not args and not opts:
            return self._fields

        if args and is_list(args[0]):
            return self.fields(*args[0], reset=True)

        c = self.clone()
        if opts.get(i("reset")):
            c._fields.reset()
        if args:
            c._fields.extend([f if isinstance(f, Expr) else Field(f) for f in args])
        return c

    def on(self, c):
        # TODO: Remove?
        self = self.clone()
        if not isinstance(self._tables, TableJoin):
            raise Error("Can't set on without join table")
        self._tables = self._tables.on(c)
        return self

    def where(self, c):
        self = self.clone()
        self._wheres = c if self._wheres is None else self._wheres & c
        return self

    def or_where(self, c):
        self = self.clone()
        self._wheres = c if self._wheres is None else self._wheres | c
        return self

    @opt_checker([i("reset"), ])
    def group_by(self, *args, **opts):
        if not args and not opts:
            return self._group_by

        if args and is_list(args[0]):
            return self.group_by(*args[0], reset=True)

        c = self.clone()
        if opts.get(i("reset")):
            c._group_by.reset()
        if args:
            c._group_by.extend(args)
        return c

    def having(self, cond):
        c = self.clone()
        c._havings = cond if c._havings is None else c._havings & cond
        return c

    def or_having(self, cond):
        c = self.clone()
        c._havings = cond if c._havings is None else c._havings | cond
        return c

    @opt_checker([i("desc"), i("reset"), ])
    def order_by(self, *args, **opts):
        if not args and not opts:
            return self._order_by

        if args and is_list(args[0]):
            return self.order_by(*args[0], reset=True)

        c = self.clone()
        if opts.get(i("reset")):
            c._order_by.reset()
        if args:
            direct = "DESC" if opts.get("desc") else "ASC"
            c._order_by.extend([f if isinstance(f, Postfix) and f._sql in ("ASC", "DESC") else Postfix(f, direct) for f in args])
        return c

    def limit(self, *args, **kwargs):
        c = self.clone()
        if args:
            if len(args) < 2:
                args = (0,) + args
            offset, limit = args
        else:
            limit = kwargs.get('limit')
            offset = kwargs.get('offset', 0)
        sql = ""
        if limit:
            sql = "LIMIT {0:d}".format(limit)
        if offset:
            sql = "{0} OFFSET {1:d}".format(sql, offset)
        c._limit = Constant(sql)
        return c

    def __getitem__(self, key):
        if isinstance(key, slice):
            offset = key.start or 0
            limit = key.stop - offset if key.stop else None
        else:
            offset, limit = key, 1
        return self.limit(offset, limit)

    @opt_checker([i("distinct"), i("for_update")])
    def select(self, *args, **opts):
        c = self.clone()
        c._action = "select"
        if args:
            c = c.fields(*args)
        if opts.get(i("distinct")):
            c = c.distinct(True)
        if opts.get(i("for_update")):
            c._for_update = True
        return c.result()

    def count(self):
        qs = type(self)().fields(Constant('COUNT')(Constant('1')).as_('count_value')).tables(self.order_by(reset=True).as_table('count_list'))
        qs._action = 'count'
        return qs.result()

    def insert(self, fv_dict, **opts):
        items = list(fv_dict.items())
        return self.insert_many([x[0] for x in items], ([x[1] for x in items], ), **opts)

    @opt_checker([i("ignore"), i("on_duplicate_key_update")])
    def insert_many(self, fields, values, **opts):
        c = self.fields(fields, reset=True)
        c._action = "insert"
        if opts.get(i("ignore")):
            c._ignore = True
        c._values = ExprList().join(", ")
        for row in values:
            c._values.append(ExprList(*row).join(", "))
        if opts.get(i("on_duplicate_key_update")):
            c._on_duplicate_key_update = ExprList().join(", ")
            for f, v in opts.get("on_duplicate_key_update").items():
                if not isinstance(f, Expr):
                    f = Field(f)
                c._on_duplicate_key_update.append(ExprList(f, Constant("="), v))
        return c.result()

    @opt_checker([i("ignore")])
    def update(self, key_values, **opts):
        c = self.clone()
        c._action = "update"
        if opts.get(i("ignore")):
            c._ignore = True
        c._key_values = ExprList().join(", ")
        for f, v in key_values.items():
            if not isinstance(f, Expr):
                f = Field(f)
            c._key_values.append(ExprList(f, Constant("="), v))
        return c.result()

    def delete(self):
        c = self.clone()
        c._action = "delete"
        return c.result()

    def as_table(self, alias):
        return self._cr.TableAlias(alias, self)

    def as_union(self):
        return self._cr.UnionQuerySet(self)

    def execute(self):
        return self.compile.sqlrepr(self)

    def result(self):
        return self.execute()

    def _sql_extend(self, sql, parts):
        for key, clause, attr in self._clauses:
            if key in parts and getattr(self, attr):
                if clause:
                    sql.append(Constant(clause))
                sql.append(getattr(self, attr))

    def _build_sql(self):
        sql = ExprList().join(" ")

        if self._action in ("select", "count"):
            sql.append(Constant("SELECT"))
            if self._distinct:
                sql.append(Constant("DISTINCT"))
            self._sql_extend(sql, ["fields", "from", "where", "group", "having", "order", "limit", ])
            if self._for_update:
                sql.append(Constant("FOR UPDATE"))

        elif self._action == "insert":
            sql.append(Constant("INSERT"))
            if self._ignore:
                sql.append(Constant("IGNORE"))
            sql.append(Constant("INTO"))
            self._sql_extend(sql, ["tables", ])
            sql.append(Parentheses(self._fields))
            sql.append(Constant("VALUES"))
            for row in self._values:
                sql.append(Parentheses(row))
            if self._on_duplicate_key_update:
                sql.append(Constant("ON DUPLICATE KEY UPDATE"))
                sql.append(self._on_duplicate_key_update)

        elif self._action == "update":
            sql.append(Constant("UPDATE"))
            if self._ignore:
                sql.append(Constant("IGNORE"))
            self._sql_extend(sql, ["tables"])
            sql.append(Constant("SET"))
            sql.append(self._key_values)
            self._sql_extend(sql, ["where", "limit", ])

        elif self._action == "delete":
            sql.append(Constant("DELETE"))
            self._sql_extend(sql, ["from", "where", ])
        return sql

    columns = same('fields')
    __copy__ = same('clone')


@compile.register(QuerySet)
def compile_queryset(compile, expr, state):
    compile(expr._build_sql(), state)


class UnionQuerySet(QuerySet):

    def __init__(self, qs):
        super(UnionQuerySet, self).__init__()
        self._union_list = ExprList(qs).join(" ")

    def __mul__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_list.append(Prefix("UNION DISTINCT", qs))
        return self

    def __add__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_list.append(Prefix("UNION ALL", qs))
        return self

    def _build_sql(self):
        sql = ExprList().join(" ")
        sql.append(self._union_list)
        self._sql_extend(sql, ["order", "limit"])
        return sql

    def clone(self):
        self = super(UnionQuerySet, self).clone()
        self._union_list = copy.copy(self._union_list)
        return self


class Name(object):

    __slots__ = (i('_name'), )

    def __init__(self, name=None):
        self._name = name


@compile.register(Name)
def compile_name(compile, expr, state):
    state.sql.append('"')
    state.sql.append(expr._name)
    state.sql.append('"')


class ClassRegistry(object):
    def __call__(self, name_or_cls):
        name = name_or_cls if isinstance(name_or_cls, string_types) else name_or_cls.__name__

        def deco(cls):
            setattr(self, name, cls)
            if not getattr(cls, '_cr', None) is self:  # save mem
                cls._cr = self
            return cls

        return deco if isinstance(name_or_cls, string_types) else deco(name_or_cls)


def is_list(v):
    return isinstance(v, (list, tuple))


def warn(old, new, stacklevel=3):
    warnings.warn("{0} is deprecated. Use {1} instead".format(old, new), PendingDeprecationWarning, stacklevel=stacklevel)

A, C, E, F, P, T, TA, QS = Alias, Condition, Expr, Field, Placeholder, Table, TableAlias, QuerySet
func = const = ConstantSpace()
qn = lambda name, compile: compile(Name(name))
cr = ClassRegistry()

for cls in (Expr, Table, TableJoin, ):
    cls.__repr__ = lambda self: "<{0}: {1}, {2}>".format(type(self).__name__, *compile.sqlrepr(self))

for cls in (Table, TableAlias, TableJoin, QuerySet, UnionQuerySet):
    cr(cls)
