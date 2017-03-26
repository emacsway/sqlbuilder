import copy
import weakref
from functools import wraps
from sqlbuilder.smartsql.constants import CONTEXT, MAX_PRECEDENCE
from sqlbuilder.smartsql.exceptions import Error

__all__ = ('Compiler', 'State', 'cached_compile', 'compile', )


class Compiler(object):

    def __init__(self, parent=None):
        self._children = weakref.WeakKeyDictionary()
        self._parents = []
        self._local_registry = {}
        self._local_precedence = {}
        self._registry = {}
        self._precedence = {}
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

    def set_precedence(self, precedence, *types):
        for type in types:
            self._local_precedence[type] = precedence
        self._update_cache()

    def _update_cache(self):
        for parent in self._parents:
            self._registry.update(parent._local_registry)
            self._precedence.update(parent._local_precedence)
        self._registry.update(self._local_registry)
        self._precedence.update(self._local_precedence)
        for child in self._children:
            child._update_cache()

    def __call__(self, expr, state=None):
        if state is None:
            state = State()
            self(expr, state)
            return ''.join(state.sql), state.params

        cls = expr.__class__
        parentheses = None
        outer_precedence = state.precedence
        inner_precedence = self.get_inner_precedence(expr)
        if inner_precedence is None:
            # pass current precedence
            # FieldList, ExprList, All, Distinct...?
            inner_precedence = outer_precedence
        state.precedence = inner_precedence
        if inner_precedence < outer_precedence:
            parentheses = True

        state.callers.insert(0, expr.__class__)

        if parentheses:
            state.sql.append('(')

        for c in cls.__mro__:
            if c in self._registry:
                self._registry[c](self, expr, state)
                break
        else:
            raise Error("Unknown compiler for {0}".format(cls))

        if parentheses:
            state.sql.append(')')
        state.callers.pop(0)
        state.precedence = outer_precedence

    def get_inner_precedence(self, cls_or_expr):
        if isinstance(cls_or_expr, type):
            cls = cls_or_expr
            if cls in self._precedence:
                return self._precedence[cls]
        else:
            expr = cls_or_expr
            cls = expr.__class__
            if hasattr(expr, 'sql'):
                try:
                    if (cls, expr.sql) in self._precedence:
                        return self._precedence[(cls, expr.sql)]
                    elif expr.sql in self._precedence:
                        return self._precedence[expr.sql]
                except TypeError:
                    # For case when expr.sql is unhashable, for example we can allow T('tablename').sql (in future).
                    pass
            return self.get_inner_precedence(cls)

        return MAX_PRECEDENCE  # self._precedence.get('(any other)', MAX_PRECEDENCE)


class State(object):

    def __init__(self):
        self.sql = []
        self.params = []
        self._stack = []
        self.callers = []
        self.auto_tables = []
        self.join_tables = []
        self.context = CONTEXT.QUERY
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


def cached_compile(f):
    @wraps(f)
    def deco(compile, expr, state):
        cache_key = (compile, state.context)
        if cache_key not in expr.__cached__:
            state.push('sql', [])
            f(compile, expr, state)
            # TODO: also cache state.tables?
            expr.__cached__[cache_key] = ''.join(state.sql)
            state.pop()
        state.sql.append(expr.__cached__[cache_key])
    return deco

compile = Compiler()
