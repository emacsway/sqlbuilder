from __future__ import absolute_import
import re
import copy
from weakref import WeakKeyDictionary

# See also:
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

except NameError:
    string_types = (str,)
    integer_types = (int,)

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


class Sql(list):

    class NotFound(IndexError):
        pass

    @classmethod
    def find_indexes(cls, step, target):
        # import pprint; print pprint.pprint((('step', step), ('target', target)))
        if type(step) == tuple:
            indexes = cls.find_indexes(step[0], target)
            return (indexes[step[1]],)
        if callable(step):
            return (step(target),)
        if isinstance(step, integer_types):
            return (step,)
        if step in (list, tuple):
            return tuple(i for i, x in enumerate(target) if type(x) == step)
        if step == '*':
            return tuple(range(len(target)))
        if isinstance(step, RE_TYPE):
            return tuple(i for i, x in enumerate(target) if isinstance(x, string_types) and step.search(x))
        return tuple(i for i, x in enumerate(target) if x == step)

    @classmethod
    def find(cls, path, target):
        step, path_rest = path[0], path[1:]
        indexes = Sql.find_indexes(step, target)
        # import pprint; print pprint.pprint((('step', step), ('path_rest', path_rest), ('indexes', indexes), ('target', target)))
        for index in indexes:
            sub_target = target[index + 1]
            if path_rest:
                try:
                    return cls.find(path_rest, sub_target)
                except IndexError:
                    # raise
                    continue
                else:
                    break
            else:
                return sub_target
        else:
            raise cls.NotFound(
                """step: {!r}, path_rest: {!r}, indexes: {!r}, target: {!r}""".format(
                    step, path_rest, indexes, target
                )
            )

    def _insert(self, path, values, strategy=lambda x: x):
        target = self.find(path[:-1], self)
        idx = self.find_indexes(path[-1], target)[0]
        idx = strategy(idx)
        target[idx:idx] = values
        return self

    def insert_after(self, path, values):
        return self._insert(path, values, lambda x: x + 1)

    def insert_before(self, path, values):
        return self._insert(path, values)

    def append_to(self, path, values):
        self.find(path, self).extend(values)
        return self

    def prepend_to(self, path, values):
        self.find(path, self)[0:0] = values
        return self
