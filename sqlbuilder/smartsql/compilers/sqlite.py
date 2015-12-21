from .. import compile as parent_compile, SPACE, Name, NameCompiler, Binary

compile = parent_compile.create_child()

TRANSLATION_MAP = {
    'LIKE': 'GLOB',
    'ILIKE': 'LIKE',
}


@compile.when(object)
def compile_object(compile, expr, state):
    state.sql.append('?')
    state.params.append(expr)


compile_name = NameCompiler(delimeter='`', escape_delimeter='`')
compile.when(Name)(compile_name)


@compile.when(Binary)
def compile_condition(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(SPACE)
    state.sql.append(TRANSLATION_MAP.get(expr._sql, expr._sql))
    state.sql.append(SPACE)
    compile(expr._right, state)
