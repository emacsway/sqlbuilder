from __future__ import absolute_import
import re
import copy
import collections
from weakref import WeakKeyDictionary

# See also:
# http://pyparsing.wikispaces.com/file/view/simpleSQL.py
# http://pyparsing.wikispaces.com/file/detail/select_parser.py
# https://wiki.postgresql.org/wiki/Query_Parsing
# https://github.com/pganalyze/queryparser/blob/master/queryparser.c
# https://github.com/lfittl/pg_query
# https://code.google.com/p/php-sql-parser/
# https://pypi.python.org/pypi/sqlparse

# Idea: parse SQL to DOM or JSON tree.

TOKEN_PATTERN = re.compile(r'^[A-Z]+(?: [A-Z]+)*$')
RE_TYPE = type(TOKEN_PATTERN)

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
    range = xrange
    from UserList import UserList

except NameError:
    string_types = (str,)
    integer_types = (int,)
    from collections import UserList

PLACEHOLDER = "%s"  # Can be re-defined by Compiler


class Error(Exception):
    pass


class State(object):

    def __init__(self):
        self.sql = []
        self.params = []
        self._stack = []
        self.callers = []
        self.precedence = 0

    def push(self, attr, new_value=None):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is None:
            new_value = copy.copy(old_value)
        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        setattr(self, *self._stack.pop(-1))


class Compiler(object):

    def __init__(self, parent=None):
        self._children = WeakKeyDictionary()
        self._parents = []
        self._local_registry = {}
        self._registry = {}
        if parent:
            self._parents.extend(parent._parents)
            self._parents.append(parent)
            parent._children[self] = True
            self._update_cache()

    def create_child(self):
        return self.__class__(self)

    def when(self, cls):
        def deco(func):
            self._local_registry[cls] = func
            self._update_cache()
            return func
        return deco

    def _update_cache(self):
        for parent in self._parents:
            self._registry.update(parent._local_registry)
        self._registry.update(self._local_registry)
        for child in self._children:
            child._update_cache()

    def __call__(self, expr, state=None):
        if state is None:
            state = State()
            self(expr, state)
            return ''.join(state.sql), state.params

        cls = expr.__class__
        for c in cls.mro():
            if c in self._registry:
                self._registry[c](self, expr, state)
                break
        else:
            raise Error("Unknown compiler for {0}".format(cls))

compile = Compiler()


@compile.when(object)
def compile_object(compile, expr, state):
    state.sql.append(PLACEHOLDER)
    state.params.append(expr)


@compile.when(type(None))
def compile_none(compile, expr, state):
    state.sql.append('NULL')


def compile_list_of_params(compile, expr, state):
    first = True
    for item in expr:
        if first:
            first = False
        else:
            state.sql.append(", ")
        compile(item, state)


def is_token(value):
    return TOKEN_PATTERN.match(value)


def get_caller(sql):
    for i in range(1, len(sql) + 1):
        caller = sql[-i].upper()
        if is_token(caller):
            break
    else:
        caller = 'QUERY'
    return caller


@compile.when(list)
@compile.when(tuple)
def compile_list(compile, expr, state):
    if Param in state.callers:
        return compile_list_of_params(compile, expr, state)

    state.push('callers')
    caller = get_caller(state.sql)
    state.callers.insert(0, caller)

    first = True
    for item in expr:
        if first:
            first = False
        else:
            if caller in ('SELECT', 'GROUP BY', 'ORDER BY'):
                state.sql.append(", ")
            else:
                state.sql.append(" ")
        compile(item, state)
    state.pop()


def compile_str(compile, expr, state):
    if Param in state.callers:
        return compile_object(compile, expr, state)
    state.sql.append(expr)

for s in string_types:
    compile.when(s)(compile_str)


class Param(object):
    __slots__ = ('_value')

    def __init__(self, value):
        self._value = value

P = Param


@compile.when(Param)
def compile_param(compile, expr, state):
    state.push('callers')
    state.callers.insert(0, Param)
    compile(expr._value, state)
    state.pop()


class Q(UserList):

    class NotFound(IndexError):
        pass

    def __init__(self, initlist=None):
        if initlist is None:
            initlist = []
        if isinstance(initlist, Q):
            self.data = initlist.data
        else:
            self.data = initlist

    def insert_after(self, path, values):
        return self._insert(path, values, lambda x: x + 1)

    def insert_before(self, path, values):
        return self._insert(path, values)

    def append_to(self, path, values):
        self.find(path).extend(values)
        return self

    def prepend_to(self, path, values):
        self.find(path)[0:0] = values
        return self

    def _insert(self, path, values, strategy=lambda x: x):
        target = self.find(path[:-1])
        idx = self.get_matcher(path[-1])(target)[0]
        idx = strategy(idx)
        target[idx:idx] = values
        return self

    def find(self, path):
        step, path_rest = path[0], path[1:]
        indexes = self.get_matcher(step)(self.data)
        for index in indexes:
            children = self.get_children_from_index(index)
            if path_rest:
                try:
                    return children.find(path_rest)
                except IndexError:
                    continue
                else:
                    break
            else:
                return children
        else:
            raise self.NotFound(
                """step: {!r}, path_rest: {!r}, indexes: {!r}, data: {!r}""".format(
                    step, path_rest, indexes, self.data
                )
            )

    def get_children_from_index(self, idx):
        max_idx = len(self.data) - 1
        i = idx
        while i <= max_idx:
            if type(self.data[i]) is list:
                return type(self)(self.data[i])
            i += 1
        raise self.NotFound

    def get_matcher(self, step):
        # Order is important!
        if isinstance(step, Matcher):
            return step
        if isinstance(step, tuple):
            return All(*map(self.get_matcher, step))
        if isinstance(step, string_types):
            return Exact(step)
        if isinstance(step, integer_types):
            return Index(step)
        if isinstance(step, slice):
            return Slice(step)
        if step is enumerate:
            return Each()
        if isinstance(step, RE_TYPE):
            return Re(step)
        if isinstance(step, type):
            return Type(step)
        if isinstance(step, collections.Callable):
            return Callable(step)
        raise Exception("Matcher not found for {!r}".format(step))

compile.when(Q)(compile_list)


class Matcher(object):

    def __init__(self, rule=None):
        self._rule = rule

    def _match_item(self, idx, item, collection):
        raise NotImplementedError

    def __call__(self, collection):
        return tuple(i for i, x in enumerate(collection) if self._match_item(i, x, collection))


class Exact(Matcher):

    def _match_item(self, idx, item, collection):
        return self._rule == item


class Type(Matcher):

    def _match_item(self, idx, item, collection):
        return type(item) == self._rule


class Index(Matcher):

    def __call__(self, collection):
        return (self._rule,)


class Slice(Matcher):

    def __init__(self, start, stop=None, step=None):
        if isinstance(start, slice):
            self._rule = start
        else:
            self._rule = slice(start, stop, step)

    def __call__(self, collection):
        return tuple(range(len(collection)))[self._rule]


class Each(Matcher):

    def _match_item(self, idx, item, collection):
        return True


class Re(Matcher):

    def __init__(self, pattern, flags=0):
        if isinstance(pattern, RE_TYPE):
            self._rule = pattern
        else:
            self._rule = re.compile(pattern, flags)

    def _match_item(self, idx, item, collection):
        return isinstance(item, string_types) and self._rule.search(item)


class Callable(Matcher):

    def _match_item(self, idx, item, collection):
        return self._rule(idx, item, collection)


class All(Matcher):

    def __init__(self, *matchers):
        self._rule = matchers

    def __call__(self, collection):
        matcher, matchers_rest = self._rule[0], self._rule[1:]
        indexes = matcher(collection)
        if matchers_rest:
            sub_collection = [collection[i] for i in indexes]
            sub_matcher = type(self)(*matchers_rest)
            sub_indexes = sub_matcher(sub_collection)
            indexes = tuple(indexes[i] for i in sub_indexes)
        return indexes


class Any(All):

    def __call__(self, collection):
        matcher, matchers_rest = self._rule[0], self._rule[1:]
        indexes = matcher(collection)
        if matchers_rest:
            next_matcher = type(self)(*matchers_rest)
            next_indexes = next_matcher(collection)
            indexes += next_indexes
            indexes = sorted(set(indexes))
        return indexes

# TODO:
# Down - skip down currrent level
# HasChild
# HasDescendant
# HasPrevSibling
# HasNextSibling
# HasPrev
# HasNext

# We don't need HasParent and HasAncestor, because it can be handled by previous steps.
# Subquery should not depend on context of usage. We don't need pass ancestors to Matcher.
