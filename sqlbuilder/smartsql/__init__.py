# -*- coding: utf-8 -*-
# Forked from http://code.google.com/p/py-smart-sql-constructor/

import sys
import copy


def sqlrepr(obj, dialect=None, *args, **kwargs):
    """Renders query set"""
    if hasattr(obj, '__sqlrepr__'):
        try:
            return obj.__sqlrepr__(*args, **kwargs)
        except:
            return obj.__sqlrepr__()
    return obj  # It's a string


def sqlparams(obj):
    """Renders query set"""
    if hasattr(obj, '__params__'):
        return obj.__params__()
    return []


class Error(Exception):
    pass


class Expr(object):

    _sql = None
    _params = None

    def __init__(self, sql, *params):
        self._sql = sql
        self._params = params

    def __eq__(self, other):
        if other is None:
            return Condition("IS", self, Expr("NULL"))

        if hasattr(other, '__iter__'):
            if len(other) < 1:
                raise Error("Empty list is not allowed")
            sql = ", ".join(["%s" for i in xrange(len(other))])
            sql = "({0})".format(sql)
            return Condition("IN", self, Expr(sql, *list(other)))

        return Condition("=", self, other)

    def __ne__(self, other):
        if other is None:
            return Condition("IS NOT", self, "NULL")

        if hasattr(other, '__iter__'):
            if len(other) < 1:
                raise Error("Empty list is not allowed")
            sql = ", ".join(["%s" for i in xrange(len(other))])
            sql = "({0})".format(sql)
            return Condition("NOT IN", self, Expr(sql, *list(other)))

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
        sql = "MOD(%s, %s)" % (sqlrepr(self), sqlrepr(other))
        params = []
        params.extent(sqlparams(self))
        params.extent(sqlparams(other))
        return Expr(sql, *params)

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

    def __mod__(self, other):
        return Condition("LIKE", self, other)

    def between(self, start, end):
        sqls = [sqlrepr(self), ]
        params = []
        if not isinstance(start, Expr):
            start = Expr("%s", start)
        sqls.append(sqlrepr(start))
        params.extent(sqlparams(start))

        if not isinstance(end, Expr):
            end = Expr("%s", end)
        sqls.append(sqlrepr(end))
        params.extent(sqlparams(end))

        sql = "{0} BETWEEN {1} AND {1}".format(*sqls)
        return Expr(sql, *params)

    def __getitem__(self, k):
        """Returns self.between()"""
        if isinstance(k, slice):
            start = k.start or 0
            end = k.stop or sys.maxint
            return self.between(start, end)
        else:
            return self.__eq__(k)

    def __sqlrepr__(self):
        return self._sql or ""

    def __params__(self):
        return self._params or []

    def __str__(self):
        return self.__sqlrepr__()

    def __repr__(self):
        return self.__sqlrepr__()


class Condition(Expr):
    def __init__(self, op, expr1, expr2):
        self.op = op.upper()

        if not isinstance(expr1, Expr):
            expr1 = Expr("%s", expr1)
        if not isinstance(expr2, Expr):
            expr1 = Expr("%s", expr2)

        self.expr1 = expr1
        self.expr2 = expr2

    def __sqlrepr__(self):
        s1 = sqlrepr(self.expr1)
        s2 = sqlrepr(self.expr2)
        if not s1:
            return s2
        if not s2:
            return s1
        if s1[0] != '(' and s1 != 'NULL' and isinstance(self.expr1, Condition):
            s1 = '(' + s1 + ')'
        if s2[0] != '(' and s2 != 'NULL' and isinstance(self.expr2, Condition):
            s2 = '(' + s2 + ')'
        return "%s %s %s" % (s1, self.op, s2)

    def __params__(self):
        params = []
        params.extend(sqlparams(self.expr1))
        params.extend(sqlparams(self.expr2))
        return params


class Prefix(Expr):

    def __init__(self, prefix, expr):
        self._prefix = prefix
        self._expr = expr

    def __sqlrepr__(self):
        return "{0} {1}".format(self._prefix, sqlrepr(self._expr))

    def __params__(self):
        return sqlparams(self._expr)

class Constant(Expr):
    def __init__(self, const, sql=None, *params):
        self._const = const
        if isinstance(sql, basestring):
            self._child = Expr(sql, params)
        else:
            self._child = sql

    def __call__(self, sql, *params):
        if isinstance(sql, basestring):
            self._child = Expr(sql, params)
        else:
            self._child = sql

    def __sqlrepr__(self):
        sql = self._const
        if self._child:
            sql = "%s(%s)" % (sql, sqlrepr(self._child))

    def __params__(self):
        return sqlparams(self._child)


class ConstantSpace:
    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError
        return Constant(attr)


class MetaTable(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        pieces = key.split("__", 1)
        name = pieces[0]
        alias = None

        if len(pieces) > 1:
            alias = pieces[1]

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

    def __sqlrepr__(self):
        sql = [self._name]

        if self._join:
            sql.insert(0, self._join)
        if self._alias:
            sql.extend(["AS", self._alias])
        if self._on:
            sql.extend(["ON", "(%s)" % (sqlrepr(self._on), )])

        return " ".join(sql)

    def __params__(self):
        return sqlparams(self._on) if self._on else []


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

    def __sqlrepr__(self):
        sql = [" ".join([sqlrepr(k) for k in self._join_list])]

        if self._join:
            sql[0] = "(%s)" % (sql[0], )
            sql.insert(0, self._join)
        if self._on:
            sql.extend(["ON", "(%s)" % (sqlrepr(self._on), )])

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
        pieces = key.split("__", 2)
        name = pieces[0]
        prefix = None
        alias = None

        if len(pieces) > 1:
            prefix = pieces[0]
            name = pieces[1]
        if len(pieces) > 2:
            alias = pieces[2]

        return cls(name, prefix, alias)


class Field(Expr):
    __metaclass__ = MetaField

    def __init__(self, name, prefix=None, alias=None):
        self._name = name
        self._prefix = prefix
        self._alias = alias

    def __sqlrepr__(self):
        sql = ".".join((self._prefix, self._name)) if self._prefix else self._name
        if self._alias:
            sql = "{0} AS {1}".format(sql, self._alias)
        return sql


class Condition2(object):
    def __init__(self, sql, params=None):
        self._sql = sql
        self._params = params if params else []

    def __and__(self, c):
        if isinstance(c, basestring):
            return self & Condition(c)

        if isinstance(c, Condition):
            return ConditionSet(self) & c

        if isinstance(c, ConditionSet):
            return c.__rand__(self)

        raise TypeError("Can't do operation with %s" % str(type(c)))

    def __or__(self, c):
        if isinstance(c, basestring):
            return self | Condition(c)

        if isinstance(c, Condition):
            return ConditionSet(self) | c

        if isinstance(c, ConditionSet):
            return c.__ror__(self)

        raise TypeError("Can't do operation with %s" % str(type(c)))

    def __sqlrepr__(self):
        return self._sql

    def __params__(self):
        return self._params


class ConditionSet(object):
    OP_AND = 0
    OP_OR = 1

    def __init__(self, c=None):
        self._empty = True
        self._last_op = None
        if c:
            self._init(c)

    def _init(self, c):
        self._sql = sqlrepr(c)
        self._params = sqlparams(c)
        if isinstance(c, ConditionSet):
            self._last_op = c._last_op
        self._empty = False
        return self

    def clone(self):
        return copy.deepcopy(self)

    def _pre_extend(self, array1, array2):
        for item in array2:
            array1.insert(0, item)

    def __rand__(self, c):
        return self.clone()._rand(c)

    def _rand(self, c):
        if isinstance(c, basestring):
            return self._rand(Condition(c))

        if not isinstance(c, Condition):
            raise TypeError("Can't do operation with %s" % str(type(c)))

        if self._empty:
            return self._init(c)

        if self._last_op is not None and self._last_op == ConditionSet.OP_OR:
            self._sql = "(%s)" % (self._sql, )

        self._sql = "%s AND %s" % (sqlrepr(c), self._sql)
        self._pre_extend(self._params, sqlparams(c))
        self._last_op = ConditionSet.OP_AND
        return self

    def __and__(self, c):
        return self.clone()._and(c)

    def _and(self, c):
        if isinstance(c, basestring):
            return self._and(Condition(c))

        if not isinstance(c, Condition) and not isinstance(c, ConditionSet):
            raise TypeError("Can't do operation with %s" % str(type(c)))

        if self._empty:
            return self._init(c)

        if self._last_op is not None and self._last_op == ConditionSet.OP_OR:
            self._sql = "(%s)" % (self._sql, )

        if isinstance(c, ConditionSet) and c._last_op == ConditionSet.OP_OR:
            self._sql = "%s AND (%s)" % (self._sql, sqlrepr(c))
        else:
            self._sql = "%s AND %s" % (self._sql, sqlrepr(c))

        self._params.extend(sqlparams(c))
        self._last_op = ConditionSet.OP_AND
        return self

    def __ror__(self, c):
        return self.clone()._ror(c)

    def _ror(self, c):
        if isinstance(c, basestring):
            return self._ror(Condition(c))

        if not isinstance(c, Condition):
            raise TypeError("Can't do operation with %s" % str(type(c)))

        if self._empty:
            return self._init(c)

        self._sql = "%s OR %s" % (sqlrepr(c), self._sql)
        self._pre_extend(self._params, sqlparams(c))
        self._last_op = ConditionSet.OP_OR
        return self

    def __or__(self, c):
        return self.clone()._or(c)

    def _or(self, c):
        if isinstance(c, basestring):
            return self._or(Condition(c))

        if not isinstance(c, Condition) and not isinstance(c, ConditionSet):
            raise TypeError("Can't do operation with %s" % str(type(c)))

        if self._empty:
            return self._init(c)

        self._sql = "%s OR %s" % (self._sql, sqlrepr(c))
        self._params.extend(sqlparams(c))
        self._last_op = ConditionSet.OP_OR
        return self

    def __sqlrepr__(self):
        return "" if self._empty else self._sql

    def __params__(self):
        return [] if self._empty else self._params


def opt_checker(k_list):
    def new_deco(func):
        def new_func(self, *args, **opt):
            for k, v in opt.items():
                if k not in k_list:
                    raise TypeError("Not implemented option: %s" % (k, ))
            return func(self, *args, **opt)

        new_func.__doc__ = func.__doc__
        return new_func
    return new_deco


def _gen_f_list(f_list, params=None):
    fields = []
    for f in f_list:
        fields.append(sqlrepr(f))
        if params is not None:
            params.extend(sqlparams(f))
    return ", ".join(fields)


def _gen_v_list(v_list, params):
    values = []
    for v in v_list:
        values.append("%s")
        params.append(v)
    return "(%s)" % (", ".join(values), )


def _gen_v_list_set(v_list_set, params):
    return ", ".join([_gen_v_list(v_list, params) for v_list in v_list_set])


def _gen_fv_dict(fv_dict, params):
    sql = []
    for f, v in fv_dict.items():
        if isinstance(v, Expr):
            sql.append("%s = %s" % (f, sqlrepr(v)))
            params.extend(sqlparams(v))
        else:
            sql.append("%s = %%s" % (f, ))
            params.append(v)

    return ", ".join(sql)


class QuerySet(object):

    def __init__(self, t):

        self.tables = t
        self._fields = []
        self._wheres = None
        self._havings = None
        self._dialect = None

        self._group_by = []
        self._order_by = []
        self._limit = None

        self._default_count_field_list = ("*", )
        self._default_count_distinct = False

    @apply
    def wheres():
        def fget(self):
            return self._wheres

        def fset(self, cs):
            self._wheres = cs

        return property(**locals())

    @apply
    def havings():
        def fget(self):
            return self._havings

        def fset(self, cs):
            self._havings = cs

        return property(**locals())

    def clone(self):
        return copy.deepcopy(self)

    def dialect(dialect=None):
        if dialect is not None:
            self._dialect = dialect
            return self
        return self._dialect

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
        if not isinstance(self.tables, TableSet):
            raise Error("Can't set on without join table")

        self.tables.on(c)
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
            sql = "LIMIT %u" % (limit, )
        if offset:
            sql = "%s OFFSET %u" % (sql, offset, )
        self._limit = sql
        return self

    def __getitem__(self, k):
        """Returns self.limit()"""
        offset = 0
        limit = None
        args = []
        if isinstance(k, slice):
            if k.start is not None:
                offset = int(k.start)
            if k.stop is not None:
                end = int(k.stop)
                limit = end - offset
        else:
            offset = k
            limit = 1
        return self.limit(offset, limit)

    @opt_checker(["distinct", "for_update"])
    def count(self, *f_list, **opt):
        self = self.clone()
        sql = ["SELECT"]
        params = []
        default_count_distinct = self._default_count_distinct

        if len(f_list) == 0:
            f_list = self._group_by
            default_count_distinct = True

        if opt.get("distinct", default_count_distinct):
            sql.append("COUNT(DISTINCT %s)" % (_gen_f_list(f_list, params), ))
        else:
            sql.append("COUNT(%s)" % (_gen_f_list(f_list, params), ))

        self._join_sql_part(sql, params, ["from", "where"])

        if opt.get("for_update"):
            sql.append("FOR UPDATE")

        return " ".join(sql), params

    @opt_checker(["distinct", "for_update"])
    def select(self, *f_list, **opt):
        self = self.clone()
        sql = ["SELECT"]
        params = []
        f_list = list(f_list)
        f_list += self._fields

        if opt.get("distinct"):
            sql.append("DISTINCT")
        sql.append(_gen_f_list(f_list, params))

        self._join_sql_part(sql, params, ["from", "where", "group", "having", "order", "limit"])

        if opt.get("for_update"):
            sql.append("FOR UPDATE")

        return " ".join(sql), params

    @opt_checker(["distinct", "for_update"])
    def select_one(self, *f_list, **opt):
        self = self.clone()
        sql = ["SELECT"]
        params = []
        f_list = list(f_list)
        f_list += self._fields

        if opt.get("distinct"):
            sql.append("DISTINCT")
        sql.append(_gen_f_list(f_list, params))

        self._join_sql_part(sql, params, ["from", "where", "group", "having", "order"])
        sql.append("LIMIT 1 OFFSET 0")

        if opt.get("for_update"):
            sql.append("FOR UPDATE")

        return " ".join(sql), params

    def select_for_union(self, *f_list):
        self = self.clone()
        return UnionPart(*self.select(*f_list))

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
        sql.append("(%s) VALUES %s" % (_gen_f_list(f_list), _gen_v_list_set(v_list_set, params)))

        fv_dict = opt.get("on_duplicate_key_update")
        if fv_dict:
            sql.append("ON DUPLICATE KEY UPDATE")
            sql.append(_gen_fv_dict(fv_dict, params))

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
        sql.append(_gen_fv_dict(fv_dict, params))

        self._join_sql_part(sql, params, ["where", "limit"])
        return " ".join(sql), params

    def delete(self):
        self = self.clone()
        sql = ["DELETE"]
        params = []

        self._join_sql_part(sql, params, ["from", "where"])
        return " ".join(sql), params

    def _join_sql_part(self, sql, params, join_list):
        if "tables" in join_list and self.tables:
            sql.append(sqlrepr(self.tables))
            params.extend(sqlparams(self.tables))
        if "from" in join_list and self.tables:
            sql.extend(["FROM", sqlrepr(self.tables)])
            params.extend(sqlparams(self.tables))
        if "where" in join_list and self._wheres:
            sql.extend(["WHERE", sqlrepr(self._wheres)])
            params.extend(sqlparams(self._wheres))
        if "group" in join_list and self._group_by:
            sql.extend(["GROUP BY", _gen_f_list(self._group_by, params)])
        if "having" in join_list and self._havings:
            sql.extend(["HAVING", sqlrepr(self._havings)])
            params.extend(sqlparams(self._havings))
        if "order" in join_list and self._order_by:
            order_by = []
            for f, direct in self._order_by:
                order_by.append("%s %s" % (sqlrepr(f), direct, ))
                params.extend(sqlparams(f))
            sql.extend(["ORDER BY", ", ".join(order_by)])
        if "limit" in join_list and self._limit:
            sql.append(self._limit)


class UnionPart(object):
    def __init__(self, sql, params):
        self._sql = sql
        self._params = params

    def __mul__(self, up):
        if not isinstance(up, UnionPart):
            raise TypeError("Can't do operation with %s" % str(type(up)))
        return UnionQuerySet(self) * up

    def __add__(self, up):
        if not isinstance(up, UnionPart):
            raise TypeError("Can't do operation with %s" % str(type(up)))
        return UnionQuerySet(self) + up

    def __sqlrepr__(self):
        return self._sql

    def __params__(self):
        return self._params


class UnionQuerySet(QuerySet):
    def __init__(self, up):
        self._union_part_list = [(None, up)]

        self._group_by = None
        self._order_by = []
        self._limit = None

    def __mul__(self, up):
        if not isinstance(up, UnionPart):
            raise TypeError("Can't do operation with %s" % str(type(up)))
        self._union_part_list.append(("UNION DISTINCT", up))
        return self

    def __add__(self, up):
        if not isinstance(up, UnionPart):
            raise TypeError("Can't do operation with %s" % str(type(up)))
        self._union_part_list.append(("UNION ALL", up))
        return self

    def select(self):
        self = self.clone()
        sql = []
        params = []

        for union_type, part in self._union_part_list:
            if union_type:
                sql.append(union_type)
            sql.append("(%s)" % (sqlrepr(part), ))

            params.extend(sqlparams(part))
            self._join_sql_part(sql, params, ["order", "limit"])

        return " ".join(sql), params

T, F, E, QS = Table, Field, Expr, QuerySet
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

    print QS(t).where(w).select(F.grade__name, F.base__img, F.lottery__price, E("(CASE 1 WHEN %s) AS exp_result", 'exp_value'))

    print
    print "*******************************************"
    print "**********  Step by Step Query2  **********"
    print "*******************************************"
    qs = QS(T.user)
    print qs.select(F.name)
    print "==========================================="
    qs.tables = (qs.tables & T.address).on(F.user__id == F.address__user_id)
    print qs.select(F.user__name, F.address__street)
    print "==========================================="
    qs.wheres = qs.wheres & (F.id == 1)
    print qs.select(F.name, F.id)
    print "==========================================="
    qs.wheres = qs.wheres & ((F.address__city_id == [111, 112]) | "address.city_id IS NULL")
    print qs.select(F.user__name, F.address__street, "COUNT(*) AS count")
    print "==========================================="

    print
    print "*******************************************"
    print "**********      Union Query      **********"
    print "*******************************************"
    a = QS(T.item).where(F.status != -1).select_for_union("type, name, img")
    b = QS(T.gift).where(F.storage > 0).select_for_union("type, name, img")
    print (a + b).order_by("type", "name", desc=True).limit(100, 10).select()

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
