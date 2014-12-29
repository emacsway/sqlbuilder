# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/
# But the code fully another... It's not a fork anymore...
from __future__ import absolute_import, unicode_literals
import sys
import copy
import warnings
from functools import partial, wraps

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

DEFAULT_DIALECT = 'postgres'
PLACEHOLDER = "%s"  # Can be re-defined by registered dialect.
LOOKUP_SEP = b'__'


class SqlDialects(object):

    __slots__ = (b'_registry', )

    def __init__(self):
        self._registry = {}

    def register(self, dialect, cls):
        def deco(func):
            self._registry.setdefault(dialect, {})[cls] = func
            return func
        return deco

    def sqlrepr(self, dialect, cls):
        ns = self._registry.setdefault(dialect, {})
        for t in cls.mro():
            r = ns.get(t, t.__dict__.get(b'__sqlrepr__'))
            if r:
                return r
        return None

sql_dialects = SqlDialects()


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
        if hasattr(other, b'__iter__'):
            return self.in_(other)
        return Condition(self, "=", other)

    def __ne__(self, other):
        if other is None:
            return self.is_not(None)
        if hasattr(other, b'__iter__'):
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

    __slots__ = (b'_sql', b'_params')

    def __init__(self, sql, *params):
        if params and hasattr(params[0], b'__iter__'):
            return self.__init__(sql, *params[0])
        self._sql, self._params = sql, params

    def __sqlrepr__(self, dialect):
        return getattr(self, b'_sql', "")

    def __params__(self):
        return getattr(self, b'_params', [])


class Condition(Expr):

    __slots__ = (b'_left', b'_right')

    def __init__(self, left, op, right):
        self._left = prepare_expr(left)
        self._sql = op.upper()
        self._right = prepare_expr(right)

    def __sqlrepr__(self, dialect):
        return "{0} {1} {2}".format(sqlrepr(self._left, dialect), self._sql, sqlrepr(self._right, dialect))

    def __params__(self):
        params = sqlparams(self._left) + sqlparams(self._right)
        return params


class ExprList(Expr):

    __slots__ = (b'_args', )

    def __init__(self, *args):
        if args and hasattr(args[0], b'__iter__'):
            return self.__init__(*args[0])
        self._sql, self._args = " ", list(map(prepare_expr, args))

    def join(self, sep):
        self._sql = sep
        return self

    def __len__(self):
        return len(self._args)

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            self._args[key] = [prepare_expr(v) for v in value]
        else:
            self._args[key] = prepare_expr(value)

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return ExprList(*self._args[start:end])
        return self._args[key]

    def __iter__(self):
        return iter(self._args)

    def append(self, x):
        return self._args.append(prepare_expr(x))

    def insert(self, i, x):
        return self._args.insert(i, prepare_expr(x))

    def extend(self, L):
        return self._args.extend([prepare_expr(v) for v in L])

    def pop(self, i):
        return self._args.pop(i)

    def remove(self, x):
        return self._args.remove(x)

    def reset(self):
        self._args = []
        return self

    def __sqlrepr__(self, dialect):
        return self._sql.join([sqlrepr(a, dialect) for a in self._args])

    def __params__(self):
        params = []
        for a in self._args:
            params.extend(sqlparams(a))
        return params

    def __copy__(self):
        dup = copy.copy(super(ExprList, self))
        dup._args = dup._args[:]
        return dup


class FieldList(ExprList):

    __slots__ = ()

    def _build(self):
        sql = ExprList().join(self._sql)
        for a in self._args:
            if isinstance(a, Alias):
                a = ExprList(a._expr, Constant("AS"), a).join(" ")
            sql.append(a)
        return sql

    def __sqlrepr__(self, dialect):
        return sqlrepr(self._build(), dialect)

    def __params__(self):
        return sqlparams(self._build())


class Concat(ExprList):

    __slots__ = (b'_args', b'_ws')

    def __init__(self, *args):
        super(Concat, self).__init__(*args)
        self._sql = ' || '
        self._ws = None

    def ws(self, sep):
        self._ws = prepare_expr(sep)
        return self

    def __sqlrepr__(self, dialect):
        value = sqlrepr(self, dialect, ExprList)
        return "concat_ws({0}, {1})".format(sqlrepr(self._ws, dialect), value) if self._ws else value

    def __params__(self):
        return sqlparams(self._ws) + super(Concat, self).__params__()


class Placeholder(Expr):

    __slots__ = ()

    def __init__(self, *params):
        super(Placeholder, self).__init__(PLACEHOLDER, *params)


class Parentheses(Expr):

    __slots__ = (b'_expr', )

    def __init__(self, expr):
        self._expr = expr

    def __sqlrepr__(self, dialect):
        return "({0})".format(sqlrepr(self._expr, dialect))

    def __params__(self):
        return sqlparams(self._expr)


class OmitParentheses(Parentheses):
    def __sqlrepr__(self, dialect):
        return sqlrepr(self._expr, dialect)


class Prefix(Expr):

    __slots__ = (b'_expr', )

    def __init__(self, prefix, expr):
        self._sql = prefix
        self._expr = prepare_expr(expr)

    def __sqlrepr__(self, dialect):
        return "{0} {1}".format(self._sql, sqlrepr(self._expr, dialect))

    def __params__(self):
        return sqlparams(self._expr)


class Postfix(Expr):

    __slots__ = (b'_expr', )

    def __init__(self, expr, postfix):
        self._sql = postfix
        self._expr = prepare_expr(expr)

    def __sqlrepr__(self, dialect):
        return "{0} {1}".format(sqlrepr(self._expr, dialect), self._sql)

    def __params__(self):
        return sqlparams(self._expr)


class Between(Expr):

    __slots__ = (b'_expr', b'_start', b'_end')

    def __init__(self, expr, start, end):
        self._expr = prepare_expr(expr)
        self._start = prepare_expr(start)
        self._end = prepare_expr(end)

    def __sqlrepr__(self, dialect):
        return "{0} BETWEEN {1} AND {2}".format(sqlrepr(self._expr, dialect), sqlrepr(self._start, dialect), sqlrepr(self._end, dialect))

    def __params__(self):
        return sqlparams(self._expr) + sqlparams(self._start) + sqlparams(self._end)


class Callable(Expr):

    __slots__ = (b'_expr', b'_args')

    def __init__(self, expr, *args):
        self._expr = expr
        self._args = ExprList(*args).join(", ")

    def __sqlrepr__(self, dialect):
        return "{0}({1})".format(sqlrepr(self._expr, dialect), sqlrepr(self._args, dialect))

    def __params__(self):
        return sqlparams(self._expr) + sqlparams(self._args)


class Constant(Expr):

    __slots__ = (b'_const', )

    def __init__(self, const):
        self._const = const.upper()

    def __call__(self, *args):
        return Callable(self, *args)

    def __sqlrepr__(self, dialect):
        return self._const


class ConstantSpace(object):

    __slots__ = ()

    def __getattr__(self, attr):
        return Constant(attr)


class MetaField(type):

    def __getattr__(cls, key):
        if key[0] == b'_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 2)
        prefix, name, alias = parts + [None] * (3 - len(parts))
        if name is None:
            prefix, name = name, prefix
        f = cls(name, prefix)
        return f.as_(alias) if alias else f


class Field(MetaField(b"NewBase", (Expr, ), {})):

    __slots__ = (b'_name', b'_prefix')

    def __init__(self, name, prefix=None):
        self._name = name
        if isinstance(prefix, string_types):
            prefix = Table(prefix)
        self._prefix = prefix

    def __cachekey__(self):
        return (self._name, self._prefix.__cachekey__())

    def __sqlrepr__(self, dialect):
        sql = self._name == '*' and self._name or qn(self._name, dialect)
        if self._prefix is not None:
            sql = ".".join((qn(self._prefix, dialect), sql))
        return sql


class Alias(Expr):

    __slots__ = (b'_expr', b'_sql')

    def __init__(self, alias, expr=None):
        self._expr = expr
        super(Alias, self).__init__(alias)

    def __sqlrepr__(self, dialect):
        return qn(self._sql, dialect)


class MetaTable(type):

    def __new__(cls, name, bases, attrs):
        def _f(attr):
            return lambda self, *a, **kw: getattr(self._cr.TableJoin(self), attr)(*a, **kw)

        for a in [b'inner_join', b'left_join', b'right_join', b'full_join', b'cross_join', b'join', b'on', b'hint']:
            attrs[a] = _f(a)
        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key[0] == b'_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        table = cls(name)
        return table.as_(alias) if alias else table


class Table(MetaTable(b"NewBase", (object, ), {})):

    __slots__ = (b'_name', )

    def __init__(self, name):
        self._name = name

    def as_(self, alias):
        return self._cr.TableAlias(alias, self)

    def __getattr__(self, name):
        if name[0] == b'_':
            raise AttributeError
        parts = name.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))
        f = Field(name, self)
        if alias:
            f = f.as_(alias)
        setattr(self, name, f)
        return f

    def __cachekey__(self):
        return self._name

    def __sqlrepr__(self, dialect):
        return qn(self._name, dialect)

    def __params__(self):
        return []

    __and__ = same(b'inner_join')
    __add__ = same(b'left_join')
    __sub__ = same(b'right_join')
    __or__ = same(b'full_join')
    __mul__ = same(b'cross_join')


class TableAlias(Table):

    __slots__ = (b'_table', b'_alias')

    def __init__(self, alias, table=None):
        self._table = table
        self._alias = alias

    def as_(self, alias):
        return type(self)(alias, self._table)

    def __cachekey__(self):
        return self._alias

    def __sqlrepr__(self, dialect):
        return qn(self._alias, dialect)


class TableJoin(object):

    __slots__ = (b'_table', b'_alias', b'_join_type', b'_on', b'_left', b'_hint', )

    def __init__(self, table_or_alias, join_type=None, on=None, left=None):
        if isinstance(table_or_alias, TableAlias):
            self._table = table_or_alias._table
            self._alias = table_or_alias
        else:
            self._table = table_or_alias
            self._alias = None
        self._join_type = join_type
        self._on = on and parentheses_conditional(on) or on
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
        self._on = parentheses_conditional(c)
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
        for a in [b'_hint', ]:
            setattr(dup, a, copy.copy(getattr(dup, a, None)))
        return dup

    def __sqlrepr__(self, dialect):
        sql = ExprList().join(" ")
        if self._left is not None:
            sql.append(self._left)
        if self._join_type:
            sql.append(Constant(self._join_type))
        if isinstance(self._table, (TableJoin, QuerySet)):
            sql.append(Parentheses(self._table))
        else:
            sql.append(self._table)
        if self._alias is not None:
            sql.extend([Constant("AS"), self._alias])
        if self._on is not None:
            sql.extend([Constant("ON"), self._on])
        if self._hint is not None:
            sql.append(self._hint)
        return sqlrepr(sql, dialect)

    def __params__(self):
        return sqlparams(self._left) + sqlparams(self._table) + sqlparams(self._on) + sqlparams(self._hint)

    as_nested = same(b'group')
    __and__ = same(b'inner_join')
    __add__ = same(b'left_join')
    __sub__ = same(b'right_join')
    __or__ = same(b'full_join')
    __mul__ = same(b'cross_join')


class QuerySet(Expr):

    _clauses = (
        ('fields', None, b'_fields'),
        ('tables', None, b'_tables'),
        ('from', 'FROM', b'_tables'),
        ('where', 'WHERE', b'_wheres'),
        ('group', 'GROUP BY', b'_group_by'),
        ('having', 'HAVING', b'_havings'),
        ('order', 'ORDER BY', b'_order_by'),
        ('limit', None, b'_limit')
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
        self._dialect = None

    def clone(self):
        dup = copy.copy(super(QuerySet, self))
        for a in [b'_fields', b'_tables', b'_group_by', b'_order_by', b'_values', b'_key_values', ]:
            setattr(dup, a, copy.copy(getattr(dup, a, None)))
        return dup

    def dialect(self, dialect=None):
        if dialect is None:
            return self._dialect
        self = self.clone()
        self._dialect = dialect
        return self

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

    @opt_checker([b"reset", ])
    def fields(self, *args, **opts):
        if not args and not opts:
            return self._fields

        if args and hasattr(args[0], b'__iter__'):
            return self.fields(*args[0], reset=True)

        c = self.clone()
        if opts.get(b"reset"):
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

    @opt_checker([b"reset", ])
    def group_by(self, *args, **opts):
        if not args and not opts:
            return self._group_by

        if args and hasattr(args[0], b'__iter__'):
            return self.group_by(*args[0], reset=True)

        c = self.clone()
        if opts.get(b"reset"):
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

    @opt_checker([b"desc", b"reset", ])
    def order_by(self, *args, **opts):
        if not args and not opts:
            return self._order_by

        if args and hasattr(args[0], b'__iter__'):
            return self.order_by(*args[0], reset=True)

        c = self.clone()
        if opts.get(b"reset"):
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
            limit = kwargs.get(b'limit')
            offset = kwargs.get(b'offset', 0)
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

    @opt_checker([b"distinct", b"for_update"])
    def select(self, *args, **opts):
        c = self.clone()
        c._action = "select"
        if args:
            c = c.fields(*args)
        if opts.get(b"distinct"):
            c = c.distinct(True)
        if opts.get(b"for_update"):
            c._for_update = True
        return c.result()

    def count(self):
        qs = type(self)().fields(Constant('COUNT')(Constant('1')).as_('count_value')).tables(self.order_by(reset=True).as_table('count_list'))
        qs._action = 'count'
        return qs.result()

    def insert(self, fv_dict, **opts):
        items = list(fv_dict.items())
        return self.insert_many([x[0] for x in items], ([x[1] for x in items], ), **opts)

    @opt_checker([b"ignore", b"on_duplicate_key_update"])
    def insert_many(self, fields, values, **opts):
        c = self.fields(fields, reset=True)
        c._action = "insert"
        if opts.get(b"ignore"):
            c._ignore = True
        c._values = ExprList().join(", ")
        for row in values:
            c._values.append(ExprList(*row).join(", "))
        if opts.get(b"on_duplicate_key_update"):
            c._on_duplicate_key_update = ExprList().join(", ")
            for f, v in opts.get("on_duplicate_key_update").items():
                if not isinstance(f, Expr):
                    f = Field(f)
                c._on_duplicate_key_update.append(ExprList(f, Constant("="), v))
        return c.result()

    @opt_checker([b"ignore"])
    def update(self, key_values, **opts):
        c = self.clone()
        c._action = "update"
        if opts.get(b"ignore"):
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
        return sqlrepr(self, self._dialect), sqlparams(self)  # as_sql()? compile()?

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

    def __sqlrepr__(self, dialect):
        sql = self._build_sql()
        return sqlrepr(sql, dialect)

    def __params__(self):
        sql = self._build_sql()
        return sqlparams(sql)

    columns = same('fields')
    __copy__ = same('clone')


class UnionQuerySet(QuerySet):

    def __init__(self, qs):
        super(UnionQuerySet, self).__init__()
        self._union_list = ExprList(Parentheses(qs)).join(" ")

    def __mul__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_list.append(Prefix("UNION DISTINCT", Parentheses(qs)))
        return self

    def __add__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_list.append(Prefix("UNION ALL", Parentheses(qs)))
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

    __slots__ = (b'_name', )

    def __init__(self, name=None):
        self._name = name

    def _sqlrepr_base(self, q, dialect):
        if hasattr(self._name, b'__sqlrepr__'):
            return sqlrepr(self._name, dialect)
        if '.' in self._name:
            return '.'.join(map(partial(qn, dialect=dialect), self._name.split('.')))
        return '{0}{1}{0}'.format(q, self._name.replace(q, ''))

    def __sqlrepr__(self, dialect):
        return self._sqlrepr_base('"', dialect)


class ClassRegistry(object):
    def __call__(self, name_or_cls):
        name = name_or_cls if isinstance(name_or_cls, string_types) else name_or_cls.__name__

        def deco(cls):
            setattr(self, name, cls)
            if not getattr(cls, '_cr', None) is self:  # save mem
                cls._cr = self
            return cls

        return deco if isinstance(name_or_cls, string_types) else deco(name_or_cls)


def placeholder_conditional(expr):
    if not isinstance(expr, (Expr, Table, TableJoin)):
        return Placeholder(expr)
    return expr


def parentheses_conditional(expr):
    if isinstance(expr, (Condition, QuerySet)) or type(expr) == Expr:
        return Parentheses(expr)
    return expr


def prepare_expr(expr):
    if expr is None:
        return Constant("NULL")
    if not isinstance(expr, Expr) and hasattr(expr, '__iter__'):
        expr = Parentheses(ExprList(*expr).join(", "))
    return parentheses_conditional(placeholder_conditional(expr))


def default_dialect(dialect=None):
    global DEFAULT_DIALECT
    if dialect is not None:
        DEFAULT_DIALECT = dialect
    return DEFAULT_DIALECT


class SqlRepr(dict):

    def __call__(self, obj, dialect=None, cls=None):
        try:
            key = (obj.__cachekey__(), dialect, cls or obj.__class__)
            return self[key]
        except AttributeError:
            return self.sqlrepr(obj, dialect, cls)
        except KeyError:
            self[key] = self.sqlrepr(obj, dialect, cls)
            return self[key]
        else:
            raise

    def sqlrepr(self, obj, dialect=None, cls=None):
        """Renders query set"""
        dialect = dialect or DEFAULT_DIALECT
        callback = sql_dialects.sqlrepr(dialect, cls or obj.__class__)
        if callback is not None:
            return callback(obj, dialect)
        return obj  # It's a string

sqlrepr = SqlRepr()


def sqlparams(obj):
    """Returns query set params"""
    if hasattr(obj, b'__params__'):
        return list(obj.__params__())
    return []


def warn(old, new, stacklevel=3):
    warnings.warn("{0} is deprecated. Use {1} instead".format(old, new), PendingDeprecationWarning, stacklevel=stacklevel)

A, C, E, F, P, T, TA, QS = Alias, Condition, Expr, Field, Placeholder, Table, TableAlias, QuerySet
func = const = ConstantSpace()
qn = lambda name, dialect: sqlrepr(Name(name), dialect)
cr = ClassRegistry()

for cls in (Expr, Table, TableJoin, ):
    cls.__repr__ = lambda self: "<{0}: {1}, {2}>".format(type(self).__name__, sqlrepr(self), sqlparams(self))

for cls in (Table, TableAlias, TableJoin, QuerySet, UnionQuerySet):
    cr(cls)

from . import dialects
