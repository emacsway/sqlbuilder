from __future__ import absolute_import, unicode_literals
# -*- coding: utf-8 -*-
# Some ideas from http://code.google.com/p/py-smart-sql-constructor/
# But the code fully another... It's not a fork anymore...
import sys
import copy
from functools import partial

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


class SqlDialects(object):
    """
    Stores all dialect representations
    """
    def __init__(self):
        """Constructor, initial registry."""
        self._registry = {}

    def register(self, dialect, cls):
        """Registers callbacks."""
        def decorator(sqlrepr_callback):
            ns = self._registry.setdefault(dialect, {})
            ns[cls] = sqlrepr_callback
            return sqlrepr_callback
        return decorator

    def sqlrepr(self, dialect, cls):
        ns = self._registry.setdefault(dialect, {})

        # Looking for registered dialect
        callback = ns.get(cls, None)
        if callback is not None:
            return callback

        # Looking for __sqlrepr__ directly in class, except parent classes
        if '__sqlrepr__' in cls.__dict__:
            return getattr(cls, '__sqlrepr__')

        # Looking for parents
        for parent in cls.__bases__:
            callback = self.sqlrepr(dialect, parent)
            if callback is not None:
                return callback
        return None

sql_dialects = SqlDialects()


def opt_checker(k_list):
    def new_deco(func):
        def new_func(self, *args, **opt):
            for k, v in list(opt.items()):
                if k not in k_list:
                    raise TypeError("Not implemented option: {0}".format(k))
            return func(self, *args, **opt)

        new_func.__doc__ = func.__doc__
        return new_func
    return new_deco


class Error(Exception):
    pass


class Expr(object):

    _sql = None
    _params = None

    def __init__(self, sql, *params):
        self._sql = sql
        self._params = []
        params = list(params)
        if len(params) and hasattr(params[0], '__iter__'):
            self._params.extend(params.pop(0))
        self._params.extend(params)

    def __eq__(self, other):
        if other is None:
            return Condition("IS", self, Constant("NULL"))
        if hasattr(other, '__iter__'):
            return self.in_(other)
        return Condition("=", self, other)

    def __ne__(self, other):
        if other is None:
            return Condition("IS NOT", self, Constant("NULL"))
        if hasattr(other, '__iter__'):
            return self.not_in(other)
        return Condition("<>", self, other)

    def __add__(self, other):
        return Condition("+", self, other)

    def __radd__(self, other):
        return Condition("+", other, self)

    def __sub__(self, other):
        return Condition("-", self, other)

    def __rsub__(self, other):
        return Condition("-", other, self)

    def __mul__(self, other):
        return Condition("*", self, other)

    def __rmul__(self, other):
        return Condition("*", other, self)

    def __div__(self, other):
        return Condition("/", self, other)

    def __rdiv__(self, other):
        return Condition("/", other, self)

    def __pos__(self):
        return Prefix("+", self)

    def __neg__(self):
        return Prefix("-", self)

    def __pow__(self, other):
        return Constant("POW")(self, other)

    def __rpow__(self, other):
        return Constant("POW")(other, self)

    def __abs__(self):
        return Constant("ABS")(self)

    def __mod__(self, other):
        return Constant("MOD")(self, other)

    def __rmod__(self, other):
        return Constant("MOD")(other, self)

    def __and__(self, other):
        return Condition("AND", self, other)

    def __or__(self, other):
        return Condition("OR", self, other)

    def __rand__(self, other):
        return Condition("AND", other, self)

    def __ror__(self, other):
        return Condition("OR", other, self)

    def __invert__(self):
        return Prefix("NOT", self)

    def __gt__(self, other):
        return Condition(">", self, other)

    def __lt__(self, other):
        return Condition("<", self, other)

    def __ge__(self, other):
        return Condition(">=", self, other)

    def __le__(self, other):
        return Condition("<=", self, other)

    def as_(self, alias):
        return Alias(alias, self)

    def in_(self, other):
        if not isinstance(other, Expr) and hasattr(other, '__iter__'):
            if len(other) < 1:
                raise Error("Empty list is not allowed")
            other = ExprList(*other).join(", ")
        return ExprList(self, Constant("IN"), Parentheses(other)).join(" ")

    def not_in(self, other):
        if not isinstance(other, Expr) and hasattr(other, '__iter__'):
            if len(other) < 1:
                raise Error("Empty list is not allowed")
            other = ExprList(*other).join(", ")
        return ExprList(self, Constant("NOT IN"), Parentheses(other)).join(" ")

    def like(self, other):
        return Condition("LIKE", self, other)

    def ilike(self, other):
        return Condition("ILIKE", self, other)

    def between(self, start, end):
        return Between(self, start, end)

    def __getitem__(self, key):
        """Returns self.between()"""
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return Between(self, start, end)
        else:
            return self.__eq__(key)

    def __sqlrepr__(self, dialect):
        return self._sql or ""

    def __params__(self):
        return self._params or []

    def __bytes__(self):
        return sqlrepr(self).encode('utf-8')

    def __str__(self):
        return sqlrepr(self)

    def __repr__(self):
        return sqlrepr(self)

    __hash__ = None

    # Aliases:
    AS = as_
    IN = in_
    NOT_IN = not_in
    LIKE = like
    BETWEEN = between


class Condition(Expr):
    def __init__(self, op, expr1, expr2):
        self._op = op.upper()
        if expr1 is not None:
            expr1 = prepare_expr(expr1)
        if expr2 is not None:
            expr2 = prepare_expr(expr2)
        self._expr1 = expr1
        self._expr2 = expr2

    def __sqlrepr__(self, dialect):
        s1 = sqlrepr(self._expr1, dialect)
        s2 = sqlrepr(self._expr2, dialect)
        if not s1:
            return s2
        if not s2:
            return s1
        return "{0} {1} {2}".format(s1, self._op, s2)

    def __params__(self):
        params = sqlparams(self._expr1) + sqlparams(self._expr2)
        return params


class ExprList(Expr):

    def __init__(self, *args):
        self._sep = " "
        self._args = []
        args = list(args)
        if len(args) and hasattr(args[0], '__iter__'):
            self._args.extend(args.pop(0))
        self._args.extend(args)

        for i, arg in enumerate(self._args):
            self._args[i] = prepare_expr(arg)

    def join(self, sep):
        self._sep = sep
        return self

    def __len__(self):
        return len(self._args)

    def __setitem__(self, key, value):
        self._args[key] = prepare_expr(value)

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxsize
            return self._args[start:end]
        else:
            return self._args[key]

    def __iter__(self):
        for a in self._args:
            yield a

    def append(self, x):
        return self._args.append(prepare_expr(x))

    def insert(self, i, x):
        return self._args.insert(i, prepare_expr(x))

    def extend(self, L):
        return self._args.extend(list(map(prepare_expr, L)))

    def pop(self, i):
        return self._args.pop(i)

    def remove(self, x):
        return self._args.remove(x)

    def reset(self):
        self._args = []
        return self

    def __sqlrepr__(self, dialect):
        sqls = []
        for arg in self._args:
            sql = sqlrepr(arg, dialect)
            # Some actions here if need
            sqls.append(sql)
        return self._sep.join(sqls)

    def __params__(self):
        params = []
        for arg in self._args:
            params.extend(sqlparams(arg))
        return params


class Placeholder(Expr):
    def __init__(self, *params):
        super(Placeholder, self).__init__(PLACEHOLDER, *params)


class Parentheses(Expr):

    def __init__(self, expr):
        self._expr = expr

    def __sqlrepr__(self, dialect):
        return "({0})".format(sqlrepr(self._expr, dialect))

    def __params__(self):
        return sqlparams(self._expr)


class Prefix(Expr):

    def __init__(self, prefix, expr):
        self._prefix = prefix
        self._expr = prepare_expr(expr)

    def __sqlrepr__(self, dialect):
        return "{0} {1}".format(self._prefix, sqlrepr(self._expr, dialect))

    def __params__(self):
        return sqlparams(self._expr)


class Between(Expr):

    def __init__(self, expr, start, end):
        self._expr = prepare_expr(expr)
        self._start = prepare_expr(start)
        self._end = prepare_expr(end)

    def __sqlrepr__(self, dialect):
        sqls = [
            sqlrepr(self._expr, dialect),
            sqlrepr(self._start, dialect),
            sqlrepr(self._end, dialect),
        ]
        return "{0} BETWEEN {1} AND {2}".format(*sqls)

    def __params__(self):
        return sqlparams(self._expr) + sqlparams(self._start) + sqlparams(self._end)


class Callable(Expr):

    def __init__(self, expr, *args):
        self._expr = expr
        self._args = ExprList(*args).join(", ")

    def __sqlrepr__(self, dialect):
        return "{0}({1})".format(sqlrepr(self._expr, dialect), sqlrepr(self._args, dialect))

    def __params__(self):
        return sqlparams(self._expr) + sqlparams(self._args)


class Constant(Expr):
    def __init__(self, const):
        self._const = const.upper()
        self._params = []

    def __call__(self, *args):
        self = copy.deepcopy(self)
        return Callable(self, *args)

    def __sqlrepr__(self, dialect):
        return self._const


class ConstantSpace:
    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError
        return Constant(attr)


class MetaField(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 2)
        name = parts[0]
        prefix = None
        alias = None

        if len(parts) > 1:
            prefix = parts[0]
            name = parts[1]
        if len(parts) > 2:
            alias = parts[2]

        f = cls(name, prefix)
        if alias is not None:
            f = f.as_(alias)
        return f


class Field(MetaField(bytes("NewBase"), (Expr, ), {})):
    def __init__(self, name, prefix=None):
        self._name = name

        if isinstance(prefix, string_types):
            prefix = Table(prefix)
        self._prefix = prefix

    def __sqlrepr__(self, dialect):
        sql = self._name == '*' and self._name or qn(self._name, dialect)
        if self._prefix is not None:
            sql = ".".join((qn(self._prefix, dialect), sql, ))
        return sql


class Alias(Expr):
    def __init__(self, alias, expr=None):
        self._expr = expr
        super(Alias, self).__init__(alias)

    @property
    def expr(self):
        return self._expr

    def __sqlrepr__(self, dialect):
        return qn(self._sql, dialect)

class Index(Alias):
    pass


class MetaTable(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split(LOOKUP_SEP, 1)
        name = parts[0]
        alias = None

        if len(parts) > 1:
            alias = parts[1]

        table = cls(name)
        if alias is not None:
            table = table.as_(alias)
        return table


class Table(MetaTable(bytes("NewBase"), (object, ), {})):
    def __init__(self, name):
        self._name = name

    def __and__(self, obj):
        return TableJoin(self).__and__(obj)

    def __add__(self, obj):
        return TableJoin(self).__add__(obj)

    def __sub__(self, obj):
        return TableJoin(self).__sub__(obj)

    def __or__(self, obj):
        return TableJoin(self).__or__(obj)

    def __mul__(self, obj):
        return TableJoin(self).__mul__(obj)

    def as_(self, alias):
        return TableAlias(alias, self)

    def on(self, c):
        return TableJoin(self).on(c)

    @opt_checker(["reset", ])
    def use_index(self, *args, **opts):
        return TableJoin(self).use_index(*args, **opts)

    @opt_checker(["reset", ])
    def ignore_index(self, *args, **opts):
        return TableJoin(self).ignore_index(*args, **opts)

    @opt_checker(["reset", ])
    def force_index(self, *args, **opts):
        return TableJoin(self).force_index(*args, **opts)

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
        parts = name.split(LOOKUP_SEP, 1)
        name = parts.pop(0)
        alias = parts.pop(0) if len(parts) else None
        f = Field(name, self)
        if alias is not None:
            f = f.as_(alias)
        return f

    def __sqlrepr__(self, dialect):
        return qn(self._name, dialect)

    def __params__(self):
        return []

    def __bytes__(self):
        return sqlrepr(self).encode('utf-8')

    def __str__(self):
        return sqlrepr(self)

    def __repr__(self):
        return sqlrepr(self)

    # Aliases:
    AS = as_
    ON = on
    USE_INDEX = use_index
    IGNORE_INDEX = ignore_index
    FORCE_INDEX = force_index


class TableAlias(Table):
    def __init__(self, alias, table):
        self._table = table
        self._alias = alias

    @property
    def table(self):
        return self._table

    def as_(self, alias):
        return TableAlias(alias, self._table)

    def __sqlrepr__(self, dialect):
        return qn(self._alias, dialect)

    # Aliases:
    AS = as_


class TableJoin(object):

    def __init__(self, table_or_alias, join_type=None, on=None, left=None):
        if isinstance(table_or_alias, TableAlias):
            self._table = table_or_alias.table
            self._alias = table_or_alias
        else:
            self._table = table_or_alias
            self._alias = None
        self._join_type = join_type
        self._on = on and parentheses_conditional(on) or on
        self._left = left
        self._use_index = ExprList().join(", ")
        self._ignore_index = ExprList().join(", ")
        self._force_index = ExprList().join(", ")

    def __and__(self, obj):
        return self._add_join("INNER JOIN", obj)

    def __add__(self, obj):
        return self._add_join("LEFT OUTER JOIN", obj)

    def __sub__(self, obj):
        return self._add_join("RIGHT OUTER JOIN", obj)

    def __or__(self, obj):
        return self._add_join("FULL OUTER JOIN", obj)

    def __mul__(self, obj):
        return self._add_join("CROSS JOIN", obj)

    def _add_join(self, join_type, obj):
        if not isinstance(obj, TableJoin) or obj.left():
            obj = TableJoin(obj, left=self)
        obj = obj.left(self).join_type(join_type)
        return obj

    def left(self, left=None):
        if left is not None:
            self._left = left
            return self
        return self._left

    def join_type(self, join_type):
        self._join_type = join_type
        return self

    def on(self, c):
        self._on = parentheses_conditional(c)
        return self

    def group(self):
        return TableJoin(self)

    def as_nested(self):
        return self.group()

    @opt_checker(["reset", ])
    def change_index(self, index, *args, **opts):
        if opts.get("reset"):
            index.reset()
        args = list(args)
        if len(args):
            if hasattr(args[0], '__iter__'):
                self = self.change_index(index, *args.pop(0), reset=True)
            if len(args):
                for i, arg in enumerate(args):
                    if isinstance(arg, string_types):
                        args[i] = Index(arg, self._table)
                index.extend(args)
                return self
        return self

    @opt_checker(["reset", ])
    def use_index(self, *args, **opts):
        return self.change_index(self._use_index, *args, **opts)

    @opt_checker(["reset", ])
    def ignore_index(self, *args, **opts):
        return self.change_index(self._ignore_index, *args, **opts)

    @opt_checker(["reset", ])
    def force_index(self, *args, **opts):
        return self.change_index(self._force_index, *args, **opts)

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
        if dialect in ('mysql', ):
            if self._use_index:
                sql.extend([Constant("USE INDEX"), Parentheses(self._use_index)])
            if self._ignore_index:
                sql.extend([Constant("USE INDEX"), Parentheses(self._ignore_index)])
            if self._force_index:
                sql.extend([Constant("USE INDEX"), Parentheses(self._force_index)])
        if self._on is not None:
            sql.extend([Constant("ON"), self._on])
        return sqlrepr(sql, dialect)

    def __params__(self):
        return sqlparams(self._left) + sqlparams(self._table) + sqlparams(self._on)

    def __bytes__(self):
        return sqlrepr(self).encode('utf-8')

    def __str__(self):
        return sqlrepr(self)

    def __repr__(self):
        return sqlrepr(self)

    # Aliases:
    ON = on
    USE_INDEX = use_index
    IGNORE_INDEX = ignore_index
    FORCE_INDEX = force_index


class QuerySet(Expr):

    def __init__(self, tables=None):

        self._distinct = False
        self._fields = ExprList().join(", ")
        if tables:
            if not isinstance(tables, TableJoin):
                tables = TableJoin(tables)
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
        self._sql = None
        self._params = []

    @property
    def wheres(self):
        return self._wheres

    @wheres.setter
    def wheres(self, cs):
        self._wheres = cs

    @property
    def havings(self):
        return self._havings

    @havings.setter
    def havings(self, cs):
        self._havings = cs

    def clone(self):
        return copy.deepcopy(self)

    def dialect(self, dialect=None):
        if dialect is not None:
            self = self.clone()
            self._dialect = dialect
            return self
        return self._dialect

    def tables(self, t=None):
        if t:
            self = self.clone()
            if not isinstance(t, TableJoin):
                t = TableJoin(t)
            self._tables = t
            return self
        return self._tables

    def distinct(self, val=None):
        if val is not None:
            self = self.clone()
            self._distinct = val
            return self
        return self._distinct

    @opt_checker(["reset", ])
    def fields(self, *args, **opts):
        if opts.get("reset"):
            self = self.clone()
            self._fields.reset()
            if not args:
                return self
        if args:
            self = self.clone()
            args = list(args)
            if hasattr(args[0], '__iter__'):
                self = self.fields(*args.pop(0), reset=True)
            if len(args):
                for i, f in enumerate(args):
                    if not isinstance(f, Expr):
                        f = Field(f)
                    if isinstance(f, Alias):
                        f = ExprList(f.expr, Constant("AS"), f).join(" ")
                    args[i] = f
                self._fields.extend(args)
            return self
        return self._fields

    def on(self, c):
        self = self.clone()
        if not isinstance(self._tables, TableJoin):
            raise Error("Can't set on without join table")
        self._tables.on(c)
        return self

    def where(self, c):
        self = self.clone()
        if self._wheres is None:
            self._wheres = c
        else:
            self.wheres = self.wheres & c
        return self

    def or_where(self, c):
        self = self.clone()
        if self._wheres is None:
            self._wheres = c
        else:
            self.wheres = self.wheres | c
        return self

    @opt_checker(["reset", ])
    def group_by(self, *args, **opts):
        if opts.get("reset"):
            self = self.clone()
            self._group_by.reset()
            if not args:
                return self
        if args:
            self = self.clone()
            args = list(args)
            if hasattr(args[0], '__iter__'):
                self = self.group_by(*args.pop(0), reset=True)
            if len(args):
                self._group_by.extend(args)
            return self
        return self._group_by

    def having(self, c):
        self = self.clone()
        if self._havings is None:
            self._havings = c
        else:
            self.havings = self.havings & c
        return self

    def or_having(self, c):
        self = self.clone()
        if self._havings is None:
            self._havings = c
        else:
            self.havings = self.havings | c
        return self

    @opt_checker(["desc", "reset", ])
    def order_by(self, *args, **opts):
        direct = Constant("DESC") if opts.get("desc") else Constant("ASC")
        if opts.get("reset"):
            self = self.clone()
            self._order_by.reset()
            if not args:
                return self
        if args:
            self = self.clone()
            args = list(args)
            if hasattr(args[0], '__iter__'):
                self = self.order_by(*args.pop(0), reset=True)
            if len(args):
                for f in args:
                    self._order_by.append(ExprList(f, direct).join(" "))
            return self
        return self._order_by

    def limit(self, *args, **kwargs):
        self = self.clone()
        limit = None
        offset = 0

        if len(args) == 1:
            limit = args[0]
        elif len(args) == 2:
            offset = args[0]
            limit = args[1]
        if len(args) > 2:
            raise Error("Too many arguments for limit.")

        if len(args) == 0:
            if 'limit' in kwargs:
                limit = kwargs['limit']
            if 'offset' in kwargs:
                offset = kwargs['offset']

        sql = ""
        if limit:
            sql = "LIMIT {0:d}".format(limit)
        if offset:
            sql = "{0} OFFSET {1:d}".format(sql, offset)
        self._limit = Constant(sql)
        return self

    def __getitem__(self, key):
        """Returns self.limit()"""
        offset = 0
        limit = None
        if isinstance(key, slice):
            if key.start is not None:
                offset = int(key.start)
            if key.stop is not None:
                end = int(key.stop)
                limit = end - offset
        else:
            offset = key
            limit = 1
        return self.limit(offset, limit)

    @opt_checker(["distinct", "for_update"])
    def count(self, *args, **opts):
        self = self.clone()
        self._action = "count"
        if len(args):
            self = self.fields(*args)
        if opts.get("distinct"):
            self = self.distinct(True)
        if opts.get("for_update"):
            self._for_update = True
        return self.result()

    @opt_checker(["distinct", "for_update"])
    def select_one(self, *args, **opts):
        return self.limit(1).select(*args, **opts)

    @opt_checker(["distinct", "for_update"])
    def select(self, *args, **opts):
        self = self.clone()
        self._action = "select"
        if len(args):
            self = self.fields(*args)
        if opts.get("distinct"):
            self = self.distinct(True)
        if opts.get("for_update"):
            self._for_update = True
        return self.result()

    def insert(self, fv_dict, **opts):
        items = list(fv_dict.items())
        return self.insert_many(
            [x[0] for x in items],
            ([x[1] for x in items], ),
            **opts
        )

    @opt_checker(["ignore", "on_duplicate_key_update"])
    def insert_many(self, fields, values, **opts):
        fields = list(fields)
        for i, f in enumerate(fields):
            if not isinstance(f, Expr):
                fields[i] = Field(f)
        self = self.fields(fields, reset=True)
        self._action = "insert"
        if opts.get("ignore"):
            self._ignore = True
        self._values = ExprList().join(", ")
        for row in values:
            self._values.append(ExprList(*row).join(", "))
        if opts.get("on_duplicate_key_update"):
            self._on_duplicate_key_update = ExprList().join(", ")
            for f, v in opts.get("on_duplicate_key_update").items():
                if not isinstance(f, Expr):
                    f = Field(f)
                self._on_duplicate_key_update.append(ExprList(f, Constant("=="), v))
        return self.result()

    @opt_checker(["ignore"])
    def update(self, key_values, **opts):
        self = self.clone()
        self._action = "update"
        if opts.get("ignore"):
            self._ignore = True
        self._key_values = ExprList().join(", ")
        for f, v in key_values.items():
            if not isinstance(f, Expr):
                f = Field(f)
            self._key_values.append(ExprList(f, Constant("=="), v))
        return self.result()

    def delete(self):
        self = self.clone()
        self._action = "delete"
        return self.result()

    def as_table(self, alias):
        return TableAlias(alias, self)

    def as_union(self):
        return UnionQuerySet(self)

    def execute(self):
        return sqlrepr(self, self._dialect), sqlparams(self)  # as_sql()? compile()?

    def result(self):
        return self.execute()

    def _sql_extend(self, sql, parts):
        if "fields" in parts and self._fields:
            sql.append(self._fields)
        if "tables" in parts and self._tables:
            sql.append(self._tables)
        if "from" in parts and self._tables:
            sql.extend([Constant("FROM"), self._tables])
        if "where" in parts and self._wheres:
            sql.extend([Constant("WHERE"), self._wheres])
        if "group" in parts and self._group_by:
            sql.extend([Constant("GROUP BY"), self._group_by])
        if "having" in parts and self._havings:
            sql.extend([Constant("HAVING"), self._havings])
        if "order" in parts and self._order_by:
            sql.extend([Constant("ORDER BY"), self._order_by])
        if "limit" in parts and self._limit:
            sql.append(self._limit)

    def _build_sql(self):
        sql = ExprList().join(" ")

        if self._action == "select":
            sql.append(Constant("SELECT"))
            if self._distinct:
                sql.append(Constant("DISTINCT"))
            self._sql_extend(sql, ["fields", "from", "where", "group", "having", "order", "limit", ])
            if self._for_update:
                sql.append(Constant("FOR UPDATE"))

        elif self._action == "count":
            sql.append(Constant("SELECT"))
            count_distinct = self._distinct
            fields = self._fields
            if len(fields) == 0:
                fields = self._group_by
                count_distinct = True
            if count_distinct:
                fields = ExprList(Constant("COUNT")(Prefix("DISTINCT", fields))).join(", ")
            else:
                fields = ExprList(Constant("COUNT")(fields)).join(", ")
            sql.append(fields)
            self._sql_extend(sql, ["from", "where", ])
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

    # Aliases:
    columns = fields


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


class Name(object):
    def __init__(self, name=None):
        self._name = name

    def __call__(self, name, dialect):
        self._name = name
        return sqlrepr(self, dialect)

    def _sqlrepr_base(self, q, dialect):
        if hasattr(self._name, '__sqlrepr__'):
            return sqlrepr(self._name, dialect)
        if '.' in self._name:
            return '.'.join(map(partial(qn, dialect=dialect), self._name.split('.')))
        return '{0}{1}{0}'.format(q, self._name.replace(q, ''))

    def __sqlrepr__(self, dialect):
        return self._sqlrepr_base('"', dialect)


def placeholder_conditional(expr):
    if not isinstance(expr, (Expr, Table, TableJoin)):
        expr = Placeholder(expr)
    return expr


def parentheses_conditional(expr):
    if isinstance(expr, (Condition, QuerySet)):
        return Parentheses(expr)
    if expr.__class__ == Expr:
        return Parentheses(expr)
    return expr


def prepare_expr(expr):
    if expr is None:
        return Constant("NULL")
    expr = placeholder_conditional(expr)
    expr = parentheses_conditional(expr)
    return expr


def default_dialect(dialect=None):
    global DEFAULT_DIALECT
    if dialect is not None:
        DEFAULT_DIALECT = dialect
    return DEFAULT_DIALECT


def sqlrepr(obj, dialect=None):
    """Renders query set"""
    if dialect is None:
        dialect = DEFAULT_DIALECT
    callback = sql_dialects.sqlrepr(dialect, obj.__class__)
    if callback is not None:
        return callback(obj, dialect)
    return obj  # It's a string


def sqlparams(obj):
    """Renders query set"""
    if hasattr(obj, '__params__'):
        return list(obj.__params__())
    return []

T, TA, F, A, E, QS = Table, TableAlias, Field, Alias, Expr, QuerySet
const = ConstantSpace()
func = const
qn = Name()

# Python 2.* compatible
try:
    unicode
except NameError:
    pass
else:
    for cls in (Expr, Table, TableJoin, ):
        cls.__unicode__ = cls.__str__
        cls.__str__ = cls.__bytes__

from . import dialects
