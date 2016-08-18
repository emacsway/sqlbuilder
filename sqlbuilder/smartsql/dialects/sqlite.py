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


compile_name = NameCompiler(delimiter='`', escape_delimiter='`')
compile.when(Name)(compile_name)


@compile.when(Binary)
def compile_condition(compile, expr, state):
    compile(expr.left, state)
    state.sql.append(SPACE)
    state.sql.append(TRANSLATION_MAP.get(expr.sql, expr.sql))
    state.sql.append(SPACE)
    compile(expr.right, state)
