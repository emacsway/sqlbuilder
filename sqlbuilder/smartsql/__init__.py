# -*- coding: utf-8 -*-
# Forked from http://code.google.com/p/py-smart-sql-constructor/
import sys
import copy

DEFAULT_DIALECT = 'postgres'
PLACEHOLDER = "%s"


class SqlDialects(object):
    """
    Stores all dialect representations
    """
    def __init__(self):
        """Constructor, initial registry."""
        self._registry = {}

    def register(self, dialect, cls, sqlrepr_callback):
        """Registers callbacks."""
        ns = self._registry.setdefault(dialect, {})
        ns[cls] = sqlrepr_callback

    def sqlrepr(self, dialect, cls):
        ns = self._registry.setdefault(dialect, {})
        callback = ns.get(cls, None)
        if callback is not None:
            return callback

        callback = getattr(cls, '__sqlrepr__', None)
        if callback is not None:
            return callback

        for parent in cls.__bases__:
            callback = self.sqlrepr(dialect, parent)
            if callback is not None:
                return callback
        return None

sql_dialects = SqlDialects()


def opt_checker(k_list):
    def new_deco(func):
        def new_func(self, *args, **opt):
            for k, v in opt.items():
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
        if isinstance(alias, basestring):
            alias = Alias(alias)
        return Condition("AS", self, alias)

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

    def between(self, start, end):
        return Between(self, start, end)

    def __getitem__(self, key):
        """Returns self.between()"""
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxint
            return Between(self, start, end)
        else:
            return self.__eq__(key)

    def __sqlrepr__(self, dialect):
        return self._sql or ""

    def __params__(self):
        return self._params or []

    def __str__(self):
        return sqlrepr(self)

    def __unicode__(self):
        return sqlrepr(self)

    def __repr__(self):
        return sqlrepr(self)

    # Aliases:
    AS = as_
    IN = in_
    NOT_IN = not_in
    LIKE = like
    BETWEEN = between

class Condition(Expr):
    def __init__(self, op, expr1, expr2):
        self._op = op.upper()
        if expr1 is not None and not isinstance(expr1, Expr):
            expr1 = Placeholder(expr1)
        if expr2 is not None and not isinstance(expr2, Expr):
            expr2 = Placeholder(expr2)

        self._expr1 = parentheses_conditional(expr1)
        self._expr2 = parentheses_conditional(expr2)

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
            if not isinstance(arg, Expr):
                self._args[i] = Placeholder(arg)
            else:
                self._args[i] = parentheses_conditional(arg)

    def join(self, sep):
        self._sep = sep
        return self

    def __len__(self):
        return len(self._args)

    def __setitem__(self, key, value):
        self._args[key] = value

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start or 0
            end = key.stop or sys.maxint
            return self._args[start:end]
        else:
            return self._args[key]

    def __getitem__(self, key):
        del self._args[key]

    def append(self, arg):
        return self._args.append(arg)

    def insert(self, key, val):
        return self._args.insert(key, val)

    def pop(self, key):
        return self._args.pop(key)

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
        self._expr = parentheses_conditional(expr)

    def __sqlrepr__(self, dialect):
        return "{0} {1}".format(self._prefix, sqlrepr(self._expr, dialect))

    def __params__(self):
        return sqlparams(self._expr)


class Between(Expr):

    def __init__(self, expr, start, end):
        if not isinstance(start, Expr):
            start = Placeholder(start)
        if not isinstance(end, Expr):
            end = Placeholder(end)
        self._expr = parentheses_conditional(expr)
        self._start = parentheses_conditional(start)
        self._end = parentheses_conditional(end)

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
        params = sqlparams(self._expr)
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


class Alias(Expr):
    pass


class ConstantSpace:
    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError
        return Constant(attr)


class MetaTable(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split("__", 1)
        name = parts[0]
        alias = None

        if len(parts) > 1:
            alias = parts[1]

        return cls(name, alias)


class Table(object):
    __metaclass__ = MetaTable

    def __init__(self, name, alias=None):
        self._name = name
        self._alias = alias
        self._join = None
        self._on = None

    def __and__(self, obj):
        return TableSet(self).__and__(obj)

    def __add__(self, obj):
        return TableSet(self).__add__(obj)

    def __sub__(self, obj):
        return TableSet(self).__sub__(obj)

    def __or__(self, obj):
        return TableSet(self).__or__(obj)

    def __mul__(self, obj):
        return TableSet(self).__mul__(obj)

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
        if self._alias:
            a = self._alias
        else:
            a = self._name
        return getattr(Field, "{0}__{1}".format(a, name))

    def __sqlrepr__(self, dialect):
        sql = [self._name]

        if self._join:
            sql.insert(0, self._join)
        if self._alias:
            sql.extend(["AS", self._alias])
        if self._on:
            sql.extend(["ON", "({0})".format(sqlrepr(self._on, dialect))])

        return " ".join(sql)

    def __params__(self):
        return sqlparams(self._on) if self._on else []


class TableAlias(Table):
    pass


class TableSet(object):
    def __init__(self, join_obj):
        self._join_list = [join_obj]

        self._sub = False
        self._join = None
        self._on = None

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

    def __sqlrepr__(self, dialect):
        sql = [" ".join([sqlrepr(k, dialect) for k in self._join_list])]

        if self._join:
            sql[0] = "({0})".format(sql[0])
            sql.insert(0, self._join)
        if self._on:
            sql.extend(["ON", "({0})".format(sqlrepr(self._on, dialect))])

        return " ".join(sql)

    def __params__(self):
        params = []
        for sql_obj in self._join_list:
            params.extend(sqlparams(sql_obj))
        return params

    def on(self, c):
        self._join_list[-1]._on = c
        return self

    def _add_join(self, join_type, obj):
        obj._join = join_type
        self._join_list.append(obj)
        return self


class MetaField(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        parts = key.split("__", 2)
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


class Field(Expr):
    __metaclass__ = MetaField

    def __init__(self, name, prefix=None):
        self._name = name

        if isinstance(prefix, basestring):
            prefix = Table(prefix)
        self._prefix = prefix

    def __sqlrepr__(self, dialect):
        sql = self._name
        if self._prefix is not None:
            sql = ".".join((sqlrepr(self._prefix, dialect), sql, ))
        return sql


class QuerySet(Expr):

    def __init__(self, tables=None):

        self._distinct = False
        self._fields = []
        self._tables = tables
        self._wheres = None
        self._havings = None
        self._dialect = DEFAULT_DIALECT

        self._group_by = []
        self._order_by = []
        self._limit = None

        self._default_count_field_list = ("*", )
        self._default_count_distinct = False

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
            self._tables = t
            return self
        return self._tables

    def distinct(self, val=None):
        if val is not None:
            self = self.clone()
            self._distinct = val
            return self
        return self._distinct

    def fields(self, *args):
        if len(args):
            self = self.clone()
            if hasattr(args[0], '__iter__'):
                self._fields = list(args[0])
            else:
                self._fields += args
            return self
        return self._fields

    def on(self, c):
        self = self.clone()
        if not isinstance(self._tables, TableSet):
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
    def group_by(self, *f_list, **opt):
        self = self.clone()
        if opt.get("reset"):
            self._group_by = []
        if len(f_list):
            if hasattr(f_list[0], '__iter__'):
                self._group_by = f_list[0]
            else:
                for f in f_list:
                    self._group_by.append(f)
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
    def order_by(self, *f_list, **opt):
        self = self.clone()
        direct = "DESC" if opt.get("desc") else "ASC"
        if opt.get("reset"):
            self._order_by = []
        if len(f_list):
            if hasattr(f_list[0], '__iter__'):
                self._order_by = f_list[0]
            else:
                for f in f_list:
                    self._order_by.append((f, direct, ))
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
        self._limit = sql
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
    def count(self, *f_list, **opt):
        self = self.clone()
        sql = ["SELECT"]
        params = []
        default_count_distinct = self._default_count_distinct or self._distinct

        if len(f_list) == 0:
            f_list = self._group_by
            default_count_distinct = True

        if opt.get("distinct", default_count_distinct):
            sql.append("COUNT(DISTINCT {0})".format(_gen_f_list(f_list, params, self._dialect)))
        else:
            sql.append("COUNT({0})".format(_gen_f_list(f_list, params, self._dialect)))

        self._join_sql_part(sql, params, ["from", "where"])

        if opt.get("for_update"):
            sql.append("FOR UPDATE")

        return " ".join(sql), params

    @opt_checker(["distinct", "for_update"])
    def select_one(self, *items, **opt):
        return self.limit(1).select(*items, **opt)

    @opt_checker(["distinct", "for_update"])
    def select(self, *f_list, **opt):
        self = self.clone()
        sql = ["SELECT"]
        params = []
        f_list = self._fields + list(f_list)

        if opt.get("distinct", self._distinct):
            sql.append("DISTINCT")
        sql.append(_gen_f_list(f_list, params, self._dialect))

        self._join_sql_part(sql, params, ["from", "where", "group", "having", "order", "limit"])

        if opt.get("for_update"):
            sql.append("FOR UPDATE")

        return " ".join(sql), params

    def insert(self, fv_dict, **opt):
        self = self.clone()
        return self.insert_many(fv_dict.keys(), ([fv_dict[k] for k in fv_dict.keys()], ), **opt)

    @opt_checker(["ignore", "on_duplicate_key_update"])
    def insert_many(self, f_list, v_list_set, **opt):
        self = self.clone()
        sql = ["INSERT"]
        params = []

        if opt.get("ignore"):
            sql.append("IGNORE")
        sql.append("INTO")

        self._join_sql_part(sql, params, ["tables"])
        sql.append("({0}) VALUES {1}".format(
            _gen_f_list(f_list, params, self._dialect),
            _gen_v_list_set(v_list_set, params))
        )

        fv_dict = opt.get("on_duplicate_key_update")
        if fv_dict:
            sql.append("ON DUPLICATE KEY UPDATE")
            sql.append(_gen_fv_dict(fv_dict, params, self._dialect))

        return " ".join(sql), params

    @opt_checker(["ignore"])
    def update(self, fv_dict, **opt):
        self = self.clone()
        sql = ["UPDATE"]
        params = []

        if opt.get("ignore"):
            sql.append("IGNORE")

        self._join_sql_part(sql, params, ["tables"])

        sql.append("SET")
        sql.append(_gen_fv_dict(fv_dict, params, self._dialect))

        self._join_sql_part(sql, params, ["where", "limit"])
        return " ".join(sql), params

    def delete(self):
        self = self.clone()
        sql = ["DELETE"]
        params = []

        self._join_sql_part(sql, params, ["from", "where"])
        return " ".join(sql), params

    def union_set(self):
        return UnionQuerySet(self)

    def _join_sql_part(self, sql, params, join_list):
        if "tables" in join_list and self._tables:
            sql.append(sqlrepr(self._tables, self._dialect))
            params.extend(sqlparams(self._tables))
        if "from" in join_list and self._tables:
            sql.extend(["FROM", sqlrepr(self._tables, self._dialect)])
            params.extend(sqlparams(self._tables))
        if "where" in join_list and self._wheres:
            sql.extend(["WHERE", sqlrepr(self._wheres, self._dialect)])
            params.extend(sqlparams(self._wheres))
        if "group" in join_list and self._group_by:
            sql.extend(["GROUP BY", _gen_f_list(self._group_by, params, self._dialect)])
        if "having" in join_list and self._havings:
            sql.extend(["HAVING", sqlrepr(self._havings, self._dialect)])
            params.extend(sqlparams(self._havings))
        if "order" in join_list and self._order_by:
            order_by = []
            for f, direct in self._order_by:
                order_by.append("{0} {1}".format(sqlrepr(f, self._dialect), direct))
                params.extend(sqlparams(f))
            sql.extend(["ORDER BY", ", ".join(order_by)])
        if "limit" in join_list and self._limit:
            sql.append(self._limit)

    def __sqlrepr__(self, dialect):
        return self.dialect(dialect).select()[0]

    def __params__(self):
        return self.select()[1]

    # Aliases:
    columns = fields


class UnionQuerySet(QuerySet):

    def __init__(self, qs):
        super(UnionQuerySet,self).__init__()
        self._union_parts = [(None, qs)]

    def __mul__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_parts.append(("UNION DISTINCT", qs))
        return self

    def __add__(self, qs):
        if not isinstance(qs, QuerySet):
            raise TypeError("Can't do operation with {0}".format(str(type(qs))))
        self._union_parts.append(("UNION ALL", qs))
        return self

    def select(self):
        self = self.clone()
        sql = []
        params = []

        for union_type, part in self._union_parts:
            if union_type:
                sql.append(union_type)
            part_sql, part_params = part.dialect(self._dialect).select()
            sql.append("({0})".format(part_sql))
            params.extend(part_params)
        self._join_sql_part(sql, params, ["order", "limit"])
        return " ".join(sql), params


def _gen_f_list(f_list, params, dialect):
    fields = []
    for f in f_list:
        fields.append(sqlrepr(f, dialect))
        if params is not None:
            params.extend(sqlparams(f))
    return ", ".join(fields)


def _gen_v_list(v_list, params):
    values = []
    for v in v_list:
        values.append(PLACEHOLDER)
        params.append(v)
    return "({0})".format(", ".join(values))


def _gen_v_list_set(v_list_set, params):
    return ", ".join([_gen_v_list(v_list, params) for v_list in v_list_set])


def _gen_fv_dict(fv_dict, params, dialect):
    sql = []
    for f, v in fv_dict.items():
        if isinstance(v, Expr):
            sql.append("{0} = {1}".format(f, sqlrepr(v, dialect)))
            params.extend(sqlparams(v))
        else:
            sql.append("{0} = {1}".format(f, PLACEHOLDER))
            params.append(v)

    return ", ".join(sql)


def parentheses_conditional(expr):
    if isinstance(expr, (Condition, QuerySet)):
        return Parentheses(expr)
    if expr.__class__ == Expr:
        return Parentheses(expr)
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

if __name__ == "__main__":

    print
    print "*******************************************"
    print "************   Single Query   *************"
    print "*******************************************"
    print QS((T.base + T.grade).on((F.base__type == F.grade__item_type) & (F.base__type == 1)) + T.lottery).on(
        F.base__type == F.lottery__item_type
    ).where(
        (F.name == "name") & (F.status == 0) | (F.name == None)
    ).group_by("base.type").having(F("count(*)") > 1).select(F.type, F.grade__grade, F.lottery__grade)

    print
    print "*******************************************"
    print "**********  Step by Step Query   **********"
    print "*******************************************"
    t = T.grade
    print QS(t).limit(0, 100).select(F.name)
    print "==========================================="

    t = (t & T.base).on(F.grade__item_type == F.base__type)
    print QS(t).order_by(F.grade__name, F.base__name, desc=True).select(F.grade__name, F.base__img)
    print "==========================================="

    t = (t + T.lottery).on(F.base__type == F.lottery__item_type)
    print QS(t).group_by(F.grade__grade).having(F.grade__grade > 0).select(F.grade__name, F.base__img, F.lottery__price)
    print "==========================================="

    w = (F.base__type == 1)
    print QS(t).where(w).select(F.grade__name, for_update=True)
    print "==========================================="

    w = w & (F.grade__status == [0, 1])
    print QS(t).where(w).group_by(F.grade__name, F.base__img).count()
    print "==========================================="

    from datetime import datetime
    w = w | (F.lottery__add_time > "2009-01-01") & (F.lottery__add_time <= datetime.now())
    print QS(t).where(w).select_one(F.grade__name, F.base__img, F.lottery__price)
    print "==========================================="

    w = w & (F.base__status != [1, 2])
    print QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, "CASE 1 WHEN 1")
    print "==========================================="

    print QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("CASE 1 WHEN " + PLACEHOLDER, 'exp_value').as_("exp_result"))

    print
    print "*******************************************"
    print "**********  Step by Step Query2  **********"
    print "*******************************************"
    qs = QS(T.user)
    print qs.select(F.name)
    print "==========================================="
    qs = qs.tables((qs.tables() & T.address).on(F.user__id == F.address__user_id))
    print qs.select(F.user__name, F.address__street)
    print "==========================================="
    qs.wheres = qs.wheres & (F.id == 1)
    print qs.select(F.name, F.id)
    print "==========================================="
    qs.wheres = qs.wheres & ((F.address__city_id == [111, 112]) | E("address.city_id IS NULL"))
    print qs.select(F.user__name, F.address__street, "COUNT(*) AS count")
    print "==========================================="

    print
    print "*******************************************"
    print "**********      SubQuery      *************"
    print "*******************************************"
    sub_q = QS(T.tb2).where(T.tb2.id == T.tb1.tb2_id).limit(1)
    print QS(T.tb1).where(T.tb1.tb2_id == sub_q).select(T.tb1.id)
    print QS(T.tb1).where(T.tb1.tb2_id.in_(sub_q)).select(T.tb1.id)
    print QS(T.tb1).select(sub_q.as_('sub_value'))
    print "SQL expression in query:"
    print QS(T.tb1).select(E('5 * 3 - 2*8').as_('sub_value'))
    print QS(T.tb1).select(E('(5 - 2) * 8 + (6 - 3) * 8').as_('sub_value'))

    print
    print "*******************************************"
    print "**********      Union Query      **********"
    print "*******************************************"
    a = QS(T.item).where(F.status != -1).fields("type, name, img")
    b = QS(T.gift).where(F.storage > 0).columns("type, name, img")
    print (a.union_set() + b).order_by("type", "name", desc=True).limit(100, 10).select()

    print
    print "*******************************************"
    print "**********    Other Operation    **********"
    print "*******************************************"
    print QS(T.user).insert({
        "name": "garfield",
        "gender": "male",
        "status": 0
    }, ignore=True)
    print "==========================================="
    fl = ("name", "gender", "status", "age")
    vl = (("garfield", "male", 0, 1), ("superwoman", "female", 0, 10))
    print QS(T.user).insert_many(fl, vl, on_duplicate_key_update={"age": E("age + VALUES(age)")})
    print "==========================================="
    print QS(T.user).where(F.id == 100).update({"name": "nobody", "status": 1}, ignore=True)
    print "==========================================="
    print QS(T.user).where(F.status == 1).delete()

    print "*******************************************"
    print "**********      Unit      **********"
    print "*******************************************"
    print "=================== ALIAS ==============="
    print QS(T.tb).where(A('al') == 5).select(T.tb.cl__al)
    print QS(T.tb).where(A('al') == 5).select(T.tb.cl.as_('al'))
    print "================== BETWEEN ================"
    print QS(T.tb).where(T.tb.cl[5:15]).select('*')
    print QS(T.tb).where(T.tb.cl[T.tb.cl2:15]).select('*')
    print QS(T.tb).where(T.tb.cl[15:T.tb.cl3]).select('*')
    print QS(T.tb).where(T.tb.cl[T.tb.cl2:T.tb.cl3]).select('*')
    print "=================== IN ==============="
    print QS(T.tb).where(T.tb.cl == [1, T.tb.cl3, 5, ]).where(T.tb.cl2 == [1, T.tb.cl4, ]).select('*')
    print QS(T.tb).where(T.tb.cl != [1, 3, 5, ]).select('*')
    print QS(T.tb).where(T.tb.cl.in_([1, 3, 5, ])).select('*')
    print QS(T.tb).where(T.tb.cl.not_in([1, 3, 5, ])).select('*')
    print "=================== CONSTANT ==============="
    print QS(T.tb).where(const.CONST_NAME == 5).select('*')
    print "=================== FUNCTION ==============="
    print QS(T.tb).where(func.FUNC_NAME(T.tb.cl) == 5).select('*')
    print QS(T.tb).where(T.tb.cl == func.RANDOM()).select('*')
    print "=================== DISTINCT ==============="
    print QS(T.tb).select('*')
    print QS(T.tb).distinct(False).select('*')
    print QS(T.tb).distinct(True).select('*')
    print "=================== MOD ==============="
    print QS(T.tb).where((T.tb.cl % 5) == 3).select('*')
    print QS(T.tb).where((T.tb.cl % T.tb.cl2) == 3).select('*')
    print QS(T.tb).where((100 % T.tb.cl2) == 3).select('*')
    print "=================== PREFIX ==============="
    print QS(T.tb).where(~T.tb.cl == 3).select('*')
    print QS(T.tb).where(Prefix((T.tb.cl == 2), (T.tb.cl2 == 3))).select('*')
