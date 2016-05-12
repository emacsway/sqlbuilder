from .. import compile as parent_compile, Name, Field, Value, ValueCompiler

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
    compile(expr._name, state)


compile_value = ValueCompiler(escape_delimeter="\\")
compile.when(Value)(compile_value)
