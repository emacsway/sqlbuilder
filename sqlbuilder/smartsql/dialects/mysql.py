from .. import (
    compile as parent_compile, SPACE, Binary, Concat, ExprList, Insert, Name,
    NameCompiler, Parentheses, Query, Value, ValueCompiler
)

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

except NameError:
    string_types = (str,)
    integer_types = (int,)

compile = parent_compile.create_child()

TRANSLATION_MAP = {
    'LIKE': 'LIKE BINARY',
    'ILIKE': 'LIKE',
}

compile_name = NameCompiler(delimiter='`', escape_delimiter='`', max_length=64)
compile.when(Name)(compile_name)

compile_value = ValueCompiler(escape_delimiter="\\")
compile.when(Value)(compile_value)


@compile.when(Binary)
def compile_condition(compile, expr, state):
    compile(expr.left, state)
    state.sql.append(SPACE)
    state.sql.append(TRANSLATION_MAP.get(expr.sql, expr.sql))
    state.sql.append(SPACE)
    compile(expr.right, state)


@compile.when(Concat)
def compile_concat(compile, expr, state):
    if not expr.ws():
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
        compile(expr.ws(), state)
        for a in expr:
            state.sql.append(expr.sql)
            compile(a, state)
        state.sql.append(')')


@compile.when(Insert)
def compile_insert(compile, expr, state):
    state.sql.append("INSERT ")
    if expr.ignore:
        state.sql.append("IGNORE ")
    state.sql.append("INTO ")
    compile(expr.table, state)
    state.sql.append(SPACE)
    compile(Parentheses(expr.fields), state)
    if isinstance(expr.values, Query):
        state.sql.append(SPACE)
        compile(expr.values, state)
    else:
        state.sql.append(" VALUES ")
        compile(ExprList(*expr.values).join(', '), state)
    if expr.on_duplicate_key_update:
        state.sql.append(" ON DUPLICATE KEY UPDATE ")
        first = True
        for f, v in expr.on_duplicate_key_update:
            if first:
                first = False
            else:
                state.sql.append(", ")
            compile(f, state)
            state.sql.append(" = ")
            compile(v, state)
