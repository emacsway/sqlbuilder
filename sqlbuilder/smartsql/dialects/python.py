import copy
import weakref
from sqlbuilder.smartsql.constants import CONTEXT
from sqlbuilder.smartsql.exceptions import Error
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.fields import Field

__all__ = ('Executor', 'State', )


class Executor(object):
    compile = compile

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

    def get_row_key(self, field):
        return self.compile(field)[0]

    def __call__(self, expr, state=None):
        cls = expr.__class__
        for c in cls.__mro__:
            if c in self._registry:
                return self._registry[c](self, expr, state)
        else:
            raise Error("Unknown executor for {0}".format(cls))

execute = Executor()


class State(object):

    def __init__(self):
        self.row = {}
        self._stack = []
        self.auto_tables = []
        self.join_tables = []
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


@execute.when(Field)
def execute_field(execute, expr, state):
    return state.row[execute.get_row_key(expr)]
