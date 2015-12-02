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

RE_TYPE = type(re.compile(""))

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
        self.callers = ['query']
        self.location = []
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
        self._local_reserved_words = {}
        self._local_group_words = {}
        self._local_list_words = {}

        self._registry = {}
        self._reserved_words = {}
        self._group_words = {}
        self._list_words = {}

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

    def add_reserved_words(self, words):
        self._local_reserved_words.update((word.lower(), True) for word in words)
        self._update_cache()

    def remove_reserved_words(self, words):
        self._local_reserved_words.update((word.lower(), None) for word in words)
        self._update_cache()

    def is_reserved_word(self, word):
        return isinstance(word, string_types) and self._reserved_words.get(word.lower()) is not None

    def add_group_words(self, words):
        self._local_group_words.update((word.lower(), True) for word in words)
        self._update_cache()

    def remove_group_words(self, words):
        self._local_group_words.update((word.lower(), None) for word in words)
        self._update_cache()

    def is_group_word(self, word):
        return isinstance(word, string_types) and self._group_words.get(word.lower()) is not None

    def add_list_words(self, words):
        self._local_list_words.update((word.lower(), True) for word in words)
        self._update_cache()

    def remove_list_words(self, words):
        self._local_list_words.update((word.lower(), None) for word in words)
        self._update_cache()

    def is_list_word(self, word):
        return isinstance(word, string_types) and self._list_words.get(word.lower()) is not None

    def _update_cache(self):
        for parent in self._parents:
            self._registry.update(parent._local_registry)
            self._reserved_words.update(parent._local_reserved_words)
            self._group_words.update(parent._local_group_words)
            self._list_words.update(parent._local_list_words)
        self._registry.update(self._local_registry)
        self._reserved_words.update(self._local_reserved_words)
        self._group_words.update(self._local_group_words)
        self._list_words.update(self._local_list_words)
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


@compile.when(list)
@compile.when(tuple)
def compile_list(compile, expr, state):
    if Param in state.callers:
        return compile_list_of_params(compile, expr, state)

    current_caller = state.callers[0]
    state.push('callers')
    state.callers.insert(0, 'expression')

    first = True
    for item in expr:
        if compile.is_reserved_word(item) and item.lower() not in ('by', 'into'):
            state.callers[0] = item.lower()
        if first:
            first = False
        else:
            if compile.is_list_word(current_caller):
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

    def append_child(self, path, values):
        for i in self.find(path):
            i.extend(values)
        return self

    def prepend_child(self, path, values):
        for i in self.find(path):
            i[0:0] = values
        return self

    def _insert(self, path, values, strategy=lambda x: x):
        for target in self.find(path[:-1]):
            for idx in self.get_matcher(path[-1])(target):
                idx = strategy(idx)
                target[idx:idx] = values
        return self

    def find(self, path):
        step, path_rest = path[0], path[1:]
        indexes = self.get_matcher(step)(self.data)
        if not indexes:
            raise self.NotFound
        result = []
        for index in indexes:
            children = self.get_children_from_index(index)
            if path_rest:
                try:
                    for i in children.find(path_rest):
                        if i not in result:
                            result.append(i)
                except self.NotFound:
                    continue
            else:
                if children not in result:
                    result.append(children)
        return result

    def get_children_from_index(self, idx):
        max_idx = len(self.data) - 1
        i = idx
        while i <= max_idx:
            if type(self.data[i]) is list:
                return type(self)(self.data[i])
            i += 1
        raise self.NotFound

    @classmethod
    def get_matcher(cls, step):
        # Order is important!
        if isinstance(step, Matcher):
            return step
        if isinstance(step, tuple):
            return Filter(*map(cls.get_matcher, step))
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

    def __init__(self, rule=None):
        if not isinstance(rule, (list, tuple, set)):
            rule = (rule,)
        self._rule = rule

    def _match_item(self, idx, item, collection):
        return type(item) in self._rule


class Index(Matcher):

    def _match_item(self, idx, item, collection):
        return idx == self._rule


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


class HasChild(Matcher):

    def _match_item(self, idx, item, collection):
        try:
            child = Q(collection).get_children_from_index(idx)
        except Q.NotFound:
            return False
        else:
            return bool(self._rule(child))


class HasDescendant(Matcher):

    def _match_item(self, idx, item, collection):
        if HasChild(self._rule):
            return True
        try:
            child = Q(collection).get_children_from_index(idx)
        except Q.NotFound:
            return False
        else:
            return bool(self(child))


class HasPrevSibling(Matcher):

    def _match_item(self, idx, item, collection):
        if idx == 0:
            return False
        return idx - 1 in self._rule(collection)


class HasNextSibling(Matcher):

    def _match_item(self, idx, item, collection):
        max_idx = len(collection) - 1
        if idx == max_idx:
            return False
        return idx + 1 in self._rule(collection)


class HasPrev(Matcher):

    def _match_item(self, idx, item, collection):
        if idx == 0:
            return False
        return bool([i for i in self._rule(collection) if i < idx])


class HasNext(Matcher):

    def _match_item(self, idx, item, collection):
        max_idx = len(collection) - 1
        if idx == max_idx:
            return False
        return bool([i for i in self._rule(collection) if i > idx])

# We don't need HasParent and HasAncestor, because it can be handled by previous steps.
# Subquery should not depend on context of usage. We don't need pass ancestors to Matcher.


class AnyLevel(Matcher):

    def _match_item(self, idx, item, collection):
        try:
            Q(collection).get_children_from_index(idx)
        except Q.NotFound:
            return idx in self._rule(collection)
        else:
            return True


class Composite(Matcher):

    def __init__(self, *matchers):
        self._rule = matchers


class Filter(Composite):

    def __call__(self, collection):
        matcher, matchers_rest = self._rule[0], self._rule[1:]
        indexes = matcher(collection)
        if matchers_rest:
            sub_collection = [collection[i] for i in indexes]
            sub_matcher = type(self)(*matchers_rest)
            sub_indexes = sub_matcher(sub_collection)
            indexes = tuple(indexes[i] for i in sub_indexes)
        return indexes


class Intersect(Composite):

    def __call__(self, collection):
        matcher, matchers_rest = self._rule[0], self._rule[1:]
        indexes = matcher(collection)
        if matchers_rest:
            next_matcher = type(self)(*matchers_rest)
            next_indexes = next_matcher(collection)
            indexes = sorted(set(indexes) & set(next_indexes))
        return indexes


class Union(Composite):

    def __call__(self, collection):
        matcher, matchers_rest = self._rule[0], self._rule[1:]
        indexes = matcher(collection)
        if matchers_rest:
            next_matcher = type(self)(*matchers_rest)
            next_indexes = next_matcher(collection)
            indexes = sorted(set(indexes) | set(next_indexes))
        return indexes


compile.add_reserved_words(
    """
    absolute action add all allocate alter and any are as asc assertion at
    authorization avg begin between bit bit_length both by cascade cascaded
    case cast catalog char character char_ length character_length check close
    coalesce collate collation column commit connect connection constraint
    constraints continue convert corresponding count create cross current
    current_date current_time current_timestamp current_ user cursor date day
    deallocate dec decimal declare default deferrable deferred delete desc
    describe descriptor diagnostics disconnect distinct domain double drop
    else end end-exec escape except exception exec execute exists external
    extract false fetch first float for foreign found from full get global go
    goto grant group having hour identity immediate in indicator initially
    inner input insensitive insert int integer intersect interval into is
    isolation join key language last leading left level like local lower
    match max min minute module month names national natural nchar next no
    not null nullif numeric octet_length of on only open option or order
    outer output overlaps pad partial position precision prepare preserve
    primary prior privileges procedure public read real references relative
    restrict revoke right rollback rows schema scroll second section select
    session session_ user set size smallint some space sql sqlcode sqlerror
    sqlstate substring sum system_user table temporary then time timestamp
    timezone_ hour timezone_minute to trailing transaction translate
    translation trim true union unique unknown update upper usage user using
    value values varchar varying view when whenever where with work write
    year zone
    """.split() + ['order by', 'group by']
)

# TODO: Should "SET" to be list word?
compile.add_list_words(
    """
    group insert order select values
    """.split() + ['order by', 'group by', 'insert into']
)
