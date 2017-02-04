from sqlbuilder.smartsql.dialects.sqlite import compile as parent_compile

compile = parent_compile.create_child()


@compile.when(object)
def compile_object(compile, expr, state):
    state.sql.append('%s')
    state.params.append(expr)
