import copy
import operator
import weakref
from sqlbuilder.smartsql.constants import CONTEXT, OPERATOR
from sqlbuilder.smartsql.expressions import Param, Name
from sqlbuilder.smartsql.exceptions import Error
from sqlbuilder.smartsql.fields import Field
from sqlbuilder.smartsql.operators import Binary
from sqlbuilder.smartsql.tables import Table
from sqlbuilder.smartsql.queries import Select

__all__ = ('Compiler', 'State', 'compile')


OPERATOR_MAPPING = {
    OPERATOR.GT: '$gt',
    OPERATOR.LT: '$lt',
    OPERATOR.GE: '$gte',
    OPERATOR.LE: '$lte',
    OPERATOR.EQ: '$eq',
    OPERATOR.NE: '$ne',
}

COMPOUND_OPERATOR_MAPPING = {
    OPERATOR.AND: '$and',
    OPERATOR.OR: '$or',
}


class Compiler(object):

    def __init__(self, parent=None):
        self._children = weakref.WeakKeyDictionary()
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

        cls = expr.__class__
        for c in cls.__mro__:
            if c in self._registry:
                return self._registry[c](self, expr, state)
        else:
            raise Error("Unknown executor for {0}".format(cls))

compile = Compiler()


class State(object):

    def __init__(self):
        self.collection_name = None
        self._stack = []
        self.context = CONTEXT.QUERY

    def push(self, attr, new_value=None):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is None:
            new_value = copy.copy(old_value)
        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        setattr(self, *self._stack.pop(-1))


def execute(query, database):
    """
    @type database: pymongo.collection.Collection
    @rtype: pymongo.cursor.CursorType
    """
    collection = database[query['$collectionName']]
    cursor = collection.find(query['$query'])
    if query.get('$orderby'):
        cursor = cursor.sort(query['$orderby'])
    return cursor


@compile.when(object)
def compile_python_builtin(compile, expr, state):
    return expr


@compile.when(Name)
def compile_field(compile, expr, state):
    return expr.name


@compile.when(Table)
def compile_field(compile, expr, state):
    if not expr._parent:
        collection_name = compile(expr._name, state)
        if state.collection_name is not None:
            assert state.collection_name == collection_name, "Different collections inside query"
        else:
            state.collection_name = collection_name
        return ''
    else:
        return compile(expr._name, state)


@compile.when(Field)
def compile_field(compile, expr, state):
    result = None
    if expr._prefix is not None and state.context != CONTEXT.FIELD_NAME:
        state.push("context", CONTEXT.FIELD_PREFIX)
        result = compile(expr._prefix, state)
        state.pop()
    if result:
        result += '.'
    result += compile(expr._name, state)
    return result


@compile.when(Param)
def compile_field(compile, expr, state):
    return expr.params


@compile.when(Binary)
def compile_field(compile, expr, state):
    if expr.sql in OPERATOR_MAPPING:
        return {compile(expr.left, state): {OPERATOR_MAPPING[expr.sql]: compile(expr.right, state)}}
    elif expr.sql in COMPOUND_OPERATOR_MAPPING:
        return {OPERATOR_MAPPING[expr.sql]: [compile(expr.left, state), compile(expr.right, state)]}
    else:
        raise Error("Unknown operator {0}".format(expr.sql))


@compile.when(Select)
def compile_field(compile, expr, state):
    result = {}
    state.push("context")
    state.context = CONTEXT.EXPR
    if expr.where():
        result['$query'] = compile(expr.where(), state)
    if expr.order_by():
        result['$orderby'] = compile(expr.order_by(), state)
    result['$collectionName'] = state.collection_name
    state.pop()
    return result

