from .. import compile as parent_compile, SPACE, Name, Field, Value

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

except NameError:
    string_types = (str,)
    integer_types = (int,)

compile = parent_compile.create_child()

@compile.when(Field)
def compile_field(compile, expr, state):
    if expr._name == '*':
        state.sql.append(expr._name)
    else:
        compile(Name(expr._name), state)

@compile.when(Value)
def compile_value(compile, expr, state):
    import pdb; pdb.set_trace()
    state.sql.append("'")
    state.sql.append(str(expr._value).replace('%', '%%').replace("'", "\\'"))
    state.sql.append("'")
