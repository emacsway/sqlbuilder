from __future__ import absolute_import
import weakref

__all__ = ('OperatorRegistry', 'operator_registry', )


class OperatorRegistry(object):

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

    def register(self, operator, operands, result_type, expression_factory):
        self._registry[(operator, operands)] = (result_type, expression_factory)
        self._update_cache()

    def get(self, operator, operands):
        try:
            return self._registry[(operator, operands)]
        except KeyError:
            # raise OperatorNotFound(operator, operands)
            from sqlbuilder.smartsql.datatypes import BaseType
            from sqlbuilder.smartsql.operators import Binary
            return (BaseType, lambda l, r: Binary(l, operator, r))

    def _update_cache(self):
        for parent in self._parents:
            self._registry.update(parent._local_registry)
        self._registry.update(self._local_registry)
        for child in self._children:
            child._update_cache()

operator_registry = OperatorRegistry()
