from __future__ import absolute_import
import copy
import types
import operator
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.constants import CONTEXT
from sqlbuilder.smartsql.exceptions import Error
from sqlbuilder.smartsql.expressions import Operable, Expr, ExprList, Constant, Parentheses, OmitParentheses, func, expr_repr
from sqlbuilder.smartsql.factory import factory
from sqlbuilder.smartsql.fields import Field, FieldList
from sqlbuilder.smartsql.operators import Asc, Desc
from sqlbuilder.smartsql.pycompat import string_types
from sqlbuilder.smartsql.tables import TableJoin
from sqlbuilder.smartsql.utils import is_list, opt_checker, same, warn

__all__ = (
    'Result', 'Executable', 'Select', 'Query', 'SelectCount', 'Raw',
    'Modify', 'Insert', 'Update', 'Delete',
    'Set', 'Union', 'Intersect', 'Except',
)

SPACE = " "


class settable(object):
    """
    alternatives:
    1. mutable keyword argument:
        query = query.where(T.author.first_name == 'Ivan', mutable=True)
    2. Select.mutable() method:
        query = query.mutable(True)
        query.where(T.author.first_name == 'Ivan')
    """

    def __init__(self, property_name):
        self._property_name = property_name
        self._method = None

    def __call__(self, method):
        self._method = method
        return self

    def __get__(self, instance, owner):
        if instance is None:
            return self._method
        return types.MethodType(self._method, instance)

    def __set__(self, instance, value):
        if is_list(self._property_name):
            for attr, val in zip(self._property_name, value):
                setattr(instance, attr, val)
        else:
            setattr(instance, self._property_name, value)


class Result(object):
    """Default implementation of Query class.

    It uses the Bridge pattern to separate implementation from interface.
    """

    compile = compile

    def __init__(self, compile=None):
        if compile is not None:
            self.compile = compile
        self._query = None

    def execute(self):
        return self.compile(self._query)

    select = count = insert = update = delete = execute

    def __call__(self, query):
        c = self  # Don't clone here to keep link to cache in this instance
        c._query = query
        return c

    def clone(self):
        c = copy.copy(super(Result, self))
        c._query = None
        return c

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        if isinstance(key, slice):
            offset = key.start or 0
            limit = key.stop - offset if key.stop else None
        else:
            offset, limit = key, 1
        return self._query.limit(offset, limit)

    __copy__ = clone


class Executable(object):

    result = Result()  # IoC

    def __init__(self, result=None):
        """ Query class.

        It uses the Bridge pattern to separate implementation from interface.

        :param result: Object of implementation.
        :type result: Result
        """
        if result is not None:
            self.result = result
        else:
            self.result = self.result.clone()

    def clone(self, *attrs):
        c = super(Executable, self).clone(*attrs)
        c.result = c.result.clone()
        return c

    def __getattr__(self, name):
        """Delegates unknown attributes to object of implementation."""
        try:
            return super(Executable, self).__getattr__(name)
        except AttributeError:
            if hasattr(self.result, name):
                attr = getattr(self.result, name)
                if isinstance(attr, types.MethodType):
                    c = self.clone()
                    return getattr(c.result(c), name)
                else:
                    return attr
            raise AttributeError


@factory.register
class Select(Expr):

    def __init__(self, tables=None):
        """ Select class.

        :param tables: tables
        :type tables: Table, TableAlias, TableJoin or None
        """
        Operable.__init__(self)
        self._distinct = ExprList().join(", ")
        self._fields = FieldList().join(", ")
        self._tables = tables
        self._where = None
        self._having = None
        self._group_by = ExprList().join(", ")
        self._order_by = ExprList().join(", ")
        self._limit = None
        self._offset = None
        self._for_update = False

    def tables(self, tables=None):
        if tables is None:
            return self._tables
        c = self.clone('_tables')
        c._tables = tables
        return c

    @opt_checker(["reset", ])
    def distinct(self, *args, **opts):
        if not args and not opts:
            return self._distinct

        if args:
            if is_list(args[0]):
                return self.distinct(*args[0], reset=True)
            elif args[0] is True and not opts.get("reset"):
                return self.distinct(*args, reset=True)
            elif args[0] is False:
                return self.distinct(reset=True)

        c = self.clone('_distinct')
        if opts.get("reset"):
            c._distinct.reset()
        if args:
            c._distinct.extend(args)
        return c

    @opt_checker(["reset", ])
    def fields(self, *args, **opts):
        # Why not name the "args" by "expressions", or "exprs" or "fields"?
        # Because it wil be a lie. The argument can be a list of expressions.
        # The name "argument" is not entirely clear, but truthful and not misleading.
        if not args and not opts:
            return self._fields

        if args and is_list(args[0]):
            return self.fields(*args[0], reset=True)

        c = self.clone('_fields')
        if opts.get("reset"):
            c._fields.reset()
        if args:
            c._fields.extend([Field(f) if isinstance(f, string_types) else f for f in args])
        return c

    def on(self, cond):
        # TODO: Remove?
        c = self.clone()
        if not isinstance(c._tables, TableJoin):
            raise Error("Can't set on without join table")
        c._tables = c._tables.on(cond)
        return c

    @settable('_where')
    def where(self, cond=None, op=operator.and_):
        if cond is None:
            return self._where
        c = self.clone()
        if c._where is None or op is None:
            c._where = cond
        else:
            c._where = op(c._where, cond)
        return c

    def or_where(self, cond):
        warn('or_where(cond)', 'where(cond, op=operator.or_)')
        return self.where(cond, op=operator.or_)

    @opt_checker(["reset", ])
    def group_by(self, *args, **opts):
        if not args and not opts:
            return self._group_by

        if args and is_list(args[0]):
            return self.group_by(*args[0], reset=True)

        c = self.clone('_group_by')
        if opts.get("reset"):
            c._group_by.reset()
        if args:
            c._group_by.extend(args)
        return c

    def having(self, cond=None, op=operator.and_):
        if cond is None:
            return self._having
        c = self.clone()
        if c._having is None or op is None:
            c._having = cond
        else:
            c._having = op(self._having, cond)
        return c

    def or_having(self, cond):
        warn('or_having(cond)', 'having(cond, op=operator.or_)')
        return self.having(cond, op=operator.or_)

    @opt_checker(["desc", "reset", ])
    def order_by(self, *args, **opts):
        if not args and not opts:
            return self._order_by

        if args and is_list(args[0]):
            return self.order_by(*args[0], reset=True)

        c = self.clone('_order_by')
        if opts.get("reset"):
            c._order_by.reset()
        if args:
            wraps = Desc if opts.get("desc") else Asc
            c._order_by.extend([f if isinstance(f, (Asc, Desc)) else wraps(f) for f in args])
        return c

    def limit(self, *args, **kwargs):
        if not args and not kwargs:
            return (self._offset, self._limit)
        c = self.clone()
        if args:
            if len(args) < 2:
                args = (0,) + args
            c._offset, c._limit = args
        else:
            c._limit = kwargs.get('limit')
            c._offset = kwargs.get('offset', 0)
        return c

    def as_table(self, alias):
        return factory.get(self).TableAlias(self, alias)

    def clone(self, *attrs):
        c = copy.copy(super(Select, self))
        # if not attrs:
        #     attrs = ('_fields', '_tables', '_group_by', '_order_by')
        for a in attrs:
            setattr(c, a, copy.copy(getattr(c, a, None)))
        return c

    columns = same('fields')
    __copy__ = same('clone')


@compile.when(Select)
def compile_query(compile, expr, state):
    state.push("auto_tables", [])  # this expr can be a subquery
    state.push("context", CONTEXT.FIELD)
    state.sql.append("SELECT ")
    if expr.distinct():
        state.sql.append("DISTINCT ")
        if expr.distinct()[0] is not True:
            state.sql.append("ON ")
            compile(Parentheses(expr._distinct), state)
            state.sql.append(SPACE)
    compile(expr.fields(), state)

    tables_sql_pos = len(state.sql)
    tables_params_pos = len(state.params)

    state.context = CONTEXT.EXPR
    if expr.where():
        state.sql.append(" WHERE ")
        compile(expr.where(), state)
    if expr.group_by():
        state.sql.append(" GROUP BY ")
        compile(expr.group_by(), state)
    if expr.having():
        state.sql.append(" HAVING ")
        compile(expr.having(), state)
    if expr.order_by():
        state.sql.append(" ORDER BY ")
        compile(expr.order_by(), state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)
    if expr._for_update:
        state.sql.append(" FOR UPDATE")

    state.push('join_tables', [])
    state.push('sql', [])
    state.push('params', [])
    state.context = CONTEXT.TABLE
    state.sql.append(" FROM ")
    compile(expr.tables(), state)
    tables_sql = state.sql
    tables_params = state.params
    state.pop()
    state.pop()
    state.pop()
    state.sql[tables_sql_pos:tables_sql_pos] = tables_sql
    state.params[tables_params_pos:tables_params_pos] = tables_params
    state.pop()
    state.pop()


@factory.register
class Query(Executable, Select):

    def __init__(self, tables=None, result=None):
        """ Query class.

        It uses the Bridge pattern to separate implementation from interface.

        :param tables: tables
        :type tables: Table, TableAlias, TableJoin or None
        :param result: Object of implementation.
        :type result: Result
        """
        Select.__init__(self, tables)
        Executable.__init__(self, result)

    @opt_checker(["distinct", "for_update"])
    def select(self, *args, **opts):
        c = self.clone()
        if args:
            c = c.fields(*args)
        if opts.get("distinct"):
            c = c.distinct(True)
        if opts.get("for_update"):
            c._for_update = True
        return c.result(c).select()
        # Never clone result. It should have back link to Query instance.
        # State of Result should be corresponding to state of Query object.
        # We need clone both Result and Query synchronously.

    def count(self, **kw):
        return self.result(SelectCount(self, **kw)).count()

    def insert(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        return self.result(factory.get(self).Insert(map=key_values, **kw)).insert()

    def insert_many(self, fields, values, **kw):
        # Deprecated
        return self.insert(fields=fields, values=values, **kw)

    def update(self, key_values=None, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('fields', self._fields)
        kw.setdefault('where', self._where)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(factory.get(self).Update(map=key_values, **kw)).update()

    def delete(self, **kw):
        kw.setdefault('table', self._tables)
        kw.setdefault('where', self._where)
        kw.setdefault('order_by', self._order_by)
        kw.setdefault('limit', self._limit)
        return self.result(factory.get(self).Delete(**kw)).delete()

    def as_set(self, all=False):
        return factory.get(self).Set(self, all=all, result=self.result)

    def set(self, *args, **kwargs):
        warn('set([all=False])', 'as_set([all=False])')
        return self.as_set(*args, **kwargs)

    def raw(self, sql, params=()):
        return factory.get(self).Raw(sql, params, result=self.result)

    def __getitem__(self, key):
        return self.result(self).__getitem__(key)

    def __len__(self):
        return self.result(self).__len__()

    def __iter__(self):
        return self.result(self).__iter__()


QuerySet = Query


@factory.register
class SelectCount(Query):

    def __init__(self, q, table_alias='count_list', field_alias='count_value'):
        Query.__init__(self, q.order_by(reset=True).as_table(table_alias))
        self._fields.append(func.Count(Constant('1')).as_(field_alias))


@factory.register
class Raw(Query):

    def __init__(self, sql, params, result=None):
        Query.__init__(self, result=result)
        self._raw = OmitParentheses(Expr(sql, params))


@compile.when(Raw)
def compile_raw(compile, expr, state):
    compile(expr._raw, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)


class Modify(object):

    def __repr__(self):
        return expr_repr(self)


@factory.register
class Insert(Modify):

    def __init__(self, table, map=None, fields=None, values=None, ignore=False, on_duplicate_key_update=None):
        self.table = table
        self.fields = FieldList(*(k if isinstance(k, Expr) else table.get_field(k) for k in (map or fields)))
        self.values = (tuple(map.values()),) if map else values
        self.ignore = ignore
        self.on_duplicate_key_update = tuple(
            (k if isinstance(k, Expr) else Field(k), v)
            for k, v in on_duplicate_key_update.items()
        ) if on_duplicate_key_update else None


@compile.when(Insert)
def compile_insert(compile, expr, state):
    state.push("context", CONTEXT.TABLE)
    state.sql.append("INSERT ")
    state.sql.append("INTO ")
    compile(expr.table, state)
    state.sql.append(SPACE)

    state.context = CONTEXT.FIELD_NAME
    compile(Parentheses(expr.fields), state)
    state.context = CONTEXT.EXPR
    if isinstance(expr.values, Query):
        state.sql.append(SPACE)
        compile(expr.values, state)
    else:
        state.sql.append(" VALUES ")
        compile(ExprList(*expr.values).join(', '), state)
    if expr.ignore:
        state.sql.append(" ON CONFLICT DO NOTHING")
    elif expr.on_duplicate_key_update:
        state.sql.append(" ON CONFLICT DO UPDATE SET ")
        first = True
        for f, v in expr.on_duplicate_key_update:
            if first:
                first = False
            else:
                state.sql.append(", ")
            state.context = CONTEXT.FIELD_NAME
            compile(f, state)
            state.context = CONTEXT.EXPR
            state.sql.append(" = ")
            compile(v, state)
    state.pop()


@factory.register
class Update(Modify):

    def __init__(self, table, map=None, fields=None, values=None, ignore=False, where=None, order_by=None, limit=None):
        self.table = table
        self.fields = FieldList(*(k if isinstance(k, Expr) else table.get_field(k) for k in (map or fields)))
        self.values = tuple(map.values()) if map else values
        self.ignore = ignore
        self.where = where
        self.order_by = order_by
        self.limit = limit


@compile.when(Update)
def compile_update(compile, expr, state):
    state.push("context", CONTEXT.TABLE)
    state.sql.append("UPDATE ")
    if expr.ignore:
        state.sql.append("IGNORE ")
    compile(expr.table, state)
    state.sql.append(" SET ")
    first = True
    for field, value in zip(expr.fields, expr.values):
        if first:
            first = False
        else:
            state.sql.append(", ")
        state.context = CONTEXT.FIELD_NAME
        compile(field, state)
        state.context = CONTEXT.EXPR
        state.sql.append(" = ")
        compile(value, state)
    state.context = CONTEXT.EXPR
    if expr.where:
        state.sql.append(" WHERE ")
        compile(expr.where, state)
    if expr.order_by:
        state.sql.append(" ORDER BY ")
        compile(expr.order_by, state)
    if expr.limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr.limit, state)
    state.pop()


@factory.register
class Delete(Modify):

    def __init__(self, table, where=None, order_by=None, limit=None):
        self.table = table
        self.where = where
        self.order_by = order_by
        self.limit = limit


@compile.when(Delete)
def compile_delete(compile, expr, state):
    state.sql.append("DELETE FROM ")
    state.push("context", CONTEXT.TABLE)
    compile(expr.table, state)
    state.context = CONTEXT.EXPR
    if expr.where:
        state.sql.append(" WHERE ")
        compile(expr.where, state)
    if expr.order_by:
        state.sql.append(" ORDER BY ")
        compile(expr.order_by, state)
    if expr.limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr.limit, state)
    state.pop()


@factory.register
class Set(Query):

    def __init__(self, *exprs, **kw):
        super(Set, self).__init__(result=kw.get('result'))
        if 'op' in kw:
            self.sql = kw['op']
        self._all = kw.get('all', False)  # Use All() instead?
        self._exprs = ExprList()
        for expr in exprs:
            self.add(expr)

    def add(self, other):
        if (isinstance(other, self.__class__) and
                other._all == self._all and
                other._limit is None and
                other._offset is None):
            for expr in other._exprs:
                self.add(expr)
            if other._for_update:
                self._for_update = other._for_update
        else:
            self._exprs.append(other)
            # TODO: reset _order_by, _for_update?

    def _op(self, cls, *others):
        if not getattr(self, 'sql', None):
            c = cls(*self._exprs, all=self._all)
            c._limit = self._limit
            c._offset = self._offset
            c._order_by = self._order_by
            c._for_update = self._for_update
        elif self.__class__ is not cls:
            c = cls(self, all=self._all)  # TODO: Should be here "all"?
        else:
            c = self.clone()
        for other in others:
            c.add(other)
        return c

    def union(self, *others):
        return self._op(factory.get(self).Union, *others)

    def intersection(self, *others):
        return self._op(factory.get(self).Intersect, *others)

    def difference(self, *others):
        return self._op(factory.get(self).Except, *others)

    # FIXME: violates the interface contract, changing the semantic of its interface
    __or__ = same('union')
    __and__ = same('intersection')
    __sub__ = same('difference')

    def all(self, all=True):
        self._all = all
        return self

    def clone(self, *attrs):
        c = Query.clone(self, *attrs)
        c._exprs = copy.copy(c._exprs)
        return c


@factory.register
class Union(Set):
    __slots__ = ()
    sql = 'UNION'


@factory.register
class Intersect(Set):
    __slots__ = ()
    sql = 'INTERSECT'


@factory.register
class Except(Set):
    __slots__ = ()
    sql = 'EXCEPT'


@compile.when(Set)
def compile_set(compile, expr, state):
    state.push("context", CONTEXT.SELECT)
    if expr._all:
        op = ' {0} ALL '.format(expr.sql)
    else:
        op = ' {0} '.format(expr.sql)
    # TODO: add tests for nested sets.
    state.precedence += 0.5  # to correct handle sub-set with limit, offset
    compile(expr._exprs.join(op), state)
    state.precedence -= 0.5
    if expr._order_by:
        # state.context = CONTEXT.FIELD_NAME
        state.context = CONTEXT.EXPR
        state.sql.append(" ORDER BY ")
        compile(expr._order_by, state)
    if expr._limit is not None:
        state.sql.append(" LIMIT ")
        compile(expr._limit, state)
    if expr._offset:
        state.sql.append(" OFFSET ")
        compile(expr._offset, state)
    if expr._for_update:
        state.sql.append(" FOR UPDATE")
    state.pop()
