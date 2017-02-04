from __future__ import absolute_import
from sqlbuilder.smartsql.compiler import compile
from sqlbuilder.smartsql.expressions import Operable, ExprList, compile_exprlist

__all__ = ('FieldList', )


class FieldList(ExprList):
    __slots__ = ()

    def __init__(self, *args):
        # if args and is_list(args[0]):
        #     return self.__init__(*args[0])
        Operable.__init__(self)
        self.sql, self.data = ", ", list(args)


@compile.when(FieldList)
def compile_fieldlist(compile, expr, state):
    # state.push('context', CONTEXT.COLUMN)
    compile_exprlist(compile, expr, state)
    # state.pop()
