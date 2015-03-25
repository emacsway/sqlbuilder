from .. import compile as parent_compile, SPACE, Name, Concat, Condition

compile = parent_compile.create_child()

TRANSLATION_MAP = {
    'LIKE': 'LIKE BINARY',
    'ILIKE': 'LIKE',
}


@compile.register(Name)
def compile_name(compile, expr, state):
    state.sql.append('`')
    state.sql.append(expr._name)
    state.sql.append('`')


@compile.register(Condition)
def compile_condition(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(SPACE)
    state.sql.append(TRANSLATION_MAP.get(expr._sql, expr._sql))
    state.sql.append(SPACE)
    compile(expr._right, state)


@compile.register(Concat)
def compile_concat(compile, expr, state):
    if not expr._ws:
        state.sql.append('CONCAT(')
        first = True
        for a in expr:
            if first:
                first = False
            else:
                state.sql.append(', ')
            compile(a, state)
        state.sql.append(')')
    else:
        state.sql.append('CONCAT_WS(')
        compile(expr._ws, state)
        for a in expr:
            state.sql.append(expr._sql)
            compile(a, state)
        state.sql.append(')')
