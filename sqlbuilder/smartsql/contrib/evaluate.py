# Based on Simple Top-Down Parser from http://effbot.org/zone/simple-top-down-parsing.htm
# This module allows mix SQL operators (like @>, &>, -|-) and python expressions.
# In other words, you can use SQL operators with Python expressions.
#
# Example of usage:
# >>> from sqlbuilder.smartsql.contrib.evaluate import compile
# >>> compile("""T.user.age <@ func.int8range(25, 30)""").evaluate(context={})
# <Binary: "user"."age" <@ INT8RANGE(%s, %s), [25, 30]>
# >>>
#
# or simple:
# >>> from sqlbuilder.smartsql.contrib.evaluate import e
# >>> e("T.user.age <@ func.int8range(25, 30)")
# <Binary: "user"."age" <@ INT8RANGE(%s, %s), [25, 30]>
# >>>

import re
from sqlbuilder import smartsql

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)

except NameError:
    string_types = (str,)
    integer_types = (int,)


class SymbolBase(object):

    name = None  # node/token type name

    def __init__(self):
        self.value = None  # used by literals
        self.first = None
        self.second = None
        self.third = None  # used by tree nodes

    def nud(self, parser):
        raise SyntaxError(
            "Syntax error (%r)." % self.name
        )

    def led(self, left, parser):
        raise SyntaxError(
            "Unknown operator (%r)." % self.name
        )

    def evaluate(self, context):
        raise NotImplementedError((self.name, vars(self)))

    def __repr__(self):
        if self.name == '(NAME)' or self.name == '(LITERAL)':
            return "(%s %s)" % (self.name[1:-1], self.value)
        out = [repr(self.name), self.first, self.second, self.third]
        out = map(str, filter(None, out))
        return '(' + ' '.join(out) + ')'


class SymbolTable(object):

    def multi(func):
        def _inner(self, names, *a, **kw):
            for name in names.split():
                func(self, name, *a, **kw)
        return _inner

    def __init__(self):
        self.symbol_table = {}

    def get(self, name, default=None):
        return self.symbol_table.get(name, default)

    def __getitem__(self, name):
        return self.symbol_table[name]

    def __iter__(self):
        return iter(self.symbol_table)

    def symbol(self, name, bp=0):
        name = name.upper()
        try:
            s = self.symbol_table[name]
        except KeyError:

            class S(SymbolBase):
                pass

            s = S
            s.__name__ = "symbol-" + name  # for debugging
            s.name = name
            s.lbp = bp
            self.symbol_table[name] = s
        else:
            s.lbp = max(bp, s.lbp)
        return s

    @multi
    def infix(self, name, bp):
        symbol = self.symbol(name, bp)

        @method(symbol)
        def led(self, left, parser):
            self.first = left
            self.second = parser.expression(bp)
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Binary(self.first.evaluate(context), self.name, self.second.evaluate(context))

    @multi
    def infix_r(self, name, bp):
        symbol = self.symbol(name, bp)

        @method(symbol)
        def led(self, left, parser):
            self.first = left
            self.second = parser.expression(bp - 0.1)
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Binary(self.first.evaluate(context), self.name, self.second.evaluate(context))

    def ternary(self, name, name2, bp):
        symbol = self.symbol(name, bp)
        symbol2 = self.symbol(name2)

        @method(symbol)
        def led(self, left, parser):
            self.first = left
            self.second = parser.expression(symbol2.lbp)
            parser.advance(symbol2.name)
            self.third = parser.expression(symbol2.lbp + 0.1)
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Ternary(
                self.first.evaluate(context),
                self.name,
                self.second.evaluate(context),
                name2,
                self.third.evaluate(context)
            )

    @multi
    def prefix(self, name, bp):
        symbol = self.symbol(name, bp)

        @method(symbol)
        def nud(self, parser):
            self.first = parser.expression(bp)
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Prefix(self.name, self.first.evaluate(context))

    @multi
    def unary(self, name, bp):
        symbol = self.symbol(name, bp)

        @method(symbol)
        def nud(self, parser):
            self.first = parser.expression(bp)
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Unary(self.name, self.first.evaluate(context))

    @multi
    def postfix(self, name, bp):
        symbol = self.symbol(name, bp)

        @method(symbol)
        def led(self, left, parser):
            self.first = left
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Postfix(self.first.evaluate(context), self.name)

    @multi
    def constant(self, name):
        symbol = self.symbol(name)

        @method(symbol)
        def nud(self, parser):
            self.name = '(LITERAL)'
            self.value = name
            return self

        @method(symbol)
        def evaluate(self, context):
            return smartsql.Constant(self.value)

symbol_table = SymbolTable()


class Lexer(object):

    _token_pattern = re.compile(r"""
        \s*
        (?:
              (
                    [.,:;()\[\]{}]
                  | [<>=+\-~^*/%&#|@]{1,3}
                  | (?<=\s)(?:IS|ISNULL|NOT|NOTNULL|IN|BETWEEN|AND|OR|OVERLAPS|LIKE|ILIKE|SIMILAR|ASC|DESC)\b
                  | \b(?:NOT)(?=\s)
              )  # operator
            | ([a-zA-Z]\w*)  # name
            | (
                    \d+(?:\.\d*)?  # number
                  | '(?:''|[^'])*'  # string
              )  # literal
        )
        """, re.U | re.I | re.S | re.X)

    def __init__(self, symbol_table):
        self.symbol_table = symbol_table

    def tokenize(self, program):
        for name, value in self._tokenize_expr(program):
            if name == '(LITERAL)':
                symbol = self.symbol_table[name]
                s = symbol()
                s.value = value
            else:  # name or operator
                symbol = symbol_table.get(value.upper())
                if symbol:  # operator or constant
                    s = symbol()
                elif name == "(NAME)":
                    symbol = symbol_table[name]
                    s = symbol()
                    s.value = value
                else:
                    raise SyntaxError("Unknown operator ({0}). Possible operators are {1!r}".format(
                        value, list(self.symbol_table)
                    ))

            yield s

    def _tokenize_expr(self, program):
        if isinstance(program, bytes):
            program = program.decode('utf-8')
        # import pprint; pprint.pprint(self._token_pattern.findall(program))
        for operator, name, literal in self._token_pattern.findall(program):
            if operator:
                yield '(operator)', operator
            elif name:
                yield '(NAME)', name
            elif literal:
                yield '(LITERAL)', literal
            else:
                raise SyntaxError
        yield '(END)', '(END)'


class Parser(object):

    token = None
    next = None

    def __init__(self, lexer):
        self.lexer = lexer

    def parse(self, program):
        generator = self.lexer.tokenize(program)
        try:
            self.next = generator.__next__  # PY3
        except AttributeError:
            self.next = generator.next
        self.token = self.next()
        return self.expression()

    def expression(self, rbp=0):
        t = self.token
        self.token = self.next()
        left = t.nud(self)
        while rbp < self.token.lbp:
            t = self.token
            self.token = self.next()
            left = t.led(left, self)
        return left

    def advance(self, name=None):
        if name and self.token.name.upper() != name.upper():
            raise SyntaxError("Expected {0!r} but found {1!r}".format(name, self.token.name))
        self.token = self.next()


def method(s):
    assert issubclass(s, SymbolBase)

    def bind(fn):
        setattr(s, fn.__name__, fn)
        return fn

    return bind

symbol, infix, infix_r, prefix, unary, postfix, ternary, constant = (
    symbol_table.symbol, symbol_table.infix, symbol_table.infix_r, symbol_table.prefix, symbol_table.unary,
    symbol_table.postfix, symbol_table.ternary, symbol_table.constant
)

symbol('(', 270)
symbol(')')
symbol('.', 270)
symbol('::', 260)
symbol('[', 250)  # array element selection
symbol(']')
unary('+ - ~', 240)
infix('^', 230)
infix('* / %', 220)
infix('+ -', 210)
infix('<< >>', 200)
infix('&', 190)
infix('#', 180)
infix('|', 170)
infix('IS', 160)
postfix('ISNULL NOTNULL', 150)

# 140 - (any other operator)  # all other native and user-defined operators
infix('<@ @> &< &> -|- && ## <-> <<| |>> &<| |&> <^ >^ ?# ?- ?| ?-| ?|| ~=', 140)
prefix('@-@ @@', 140)
postfix('ASC DESC', 140)

infix('IN', 130)
ternary('BETWEEN', 'AND', 120)
infix('OVERLAPS', 110)

infix('LIKE ILIKE SIMILAR', 100)
infix('< >', 90)
infix('<= >= <> !=', 80)
infix('=', 70)
prefix('NOT', 60)
infix('AND', 50)
infix('OR', 40)

symbol(',')
symbol('(NAME)')
symbol('(LITERAL)')
symbol('(END)')

constant('NULL TRUE FALSE')


@method(symbol('(NAME)'))
def nud(self, parser):
    return self


@method(symbol('(NAME)'))
def evaluate(self, context):
    if self.value in context:
        return context[self.value]
    elif hasattr(smartsql, self.value):
        return getattr(smartsql, self.value)
    else:
        raise SyntaxError("Unknown name {0!r}. Possible names are {1!r}".format(
            self.value, list(context.keys()) + list(dir(smartsql))
        ))


@method(symbol('(LITERAL)'))
def nud(self, parser):
    return self


@method(symbol('(LITERAL)'))
def evaluate(self, context):
    if self.value.isnumeric():
        return int(self.value)
    else:
        return self.value.strip("'").replace("''", "'")  # string literal enclosed by single quotes


@method(symbol('.'))
def led(self, left, parser):
    if parser.token.name != '(NAME)':
        SyntaxError("Expected an attribute name.")
    self.first = left
    self.second = parser.token
    parser.advance()
    return self


@method(symbol('.'))
def evaluate(self, context):
    return getattr(self.first.evaluate(context), self.second.value)


# Parentheses
@method(symbol('('))
def nud(self, parser):
    # parenthesized form; replaced by tuple former below
    expr = parser.expression()
    parser.advance(')')
    return expr


# Function Calls
@method(symbol('('))
def led(self, left, parser):
    self.first = left
    self.second = []
    if parser.token.name != ')':
        while 1:
            self.second.append(parser.expression())
            if parser.token.name != ',':
                break
            parser.advance(',')
    parser.advance(')')
    return self


@method(symbol('('))
def evaluate(self, context):
    return self.first.evaluate(context)(*[i.evaluate(context) for i in self.second])


@method(symbol('('))
def nud(self, parser):
    self.first = []
    comma = False
    if parser.token.name != ')':
        while 1:
            if parser.token.name == ')':
                break
            self.first.append(parser.expression())
            if parser.token.name != ',':
                break
            comma = True
            parser.advance(',')
    parser.advance(')')
    if not self.first or comma:
        return self  # tuple
    else:
        return self.first[0]


# multitoken operators

@method(symbol('NOT'))
def led(self, left, parser):
    if parser.token.name not in ('IN', 'BETWEEN'):
        raise SyntaxError("Invalid syntax")
    name2 = parser.token.name
    parser.advance()
    self.name += ' ' + name2
    self.__class__ = symbol(name2)  # PY2
    symbol(name2).led(self, left, parser)
    return self


@method(symbol('NOT'))
def evaluate(self, context):
    if ' ' in self.name:
        self.__class__ = symbol(self.name.split().pop())  # PY2
        return symbol(self.name.split().pop()).evaluate(self, context)
    else:
        return smartsql.Prefix(self.name, self.first.evaluate(context))


@method(symbol('IS'))
def led(self, left, parser):
    if parser.token.name == 'NOT':
        name2 = parser.token.name
        parser.advance()
        self.name += ' ' + name2
    self.first = left
    self.second = parser.expression(self.lbp)
    return self


# Array
@method(symbol('['))
def nud(self, parser):
    self.first = []
    if parser.token.name != ']':
        while 1:
            if parser.token.name == ']':
                break
            self.first.append(parser.expression())
            if parser.token.name != ',':
                break
            parser.advance(',')
    parser.advance(']')
    return self


# JSON
@method(symbol('{'))
def nud(self, parser):
    self.first = []
    if parser.token.name != '}':
        while 1:
            if parser.token.name == '}':
                break
            self.first.append(parser.expression())
            parser.advance(':')
            self.first.append(parser.expression())
            if parser.token.name != ',':
                break
            parser.advance(',')
    parser.advance('}')
    return self

symbol('}')


class Compiler(object):

    parser_factory = Parser
    lexer_factory = Lexer
    symbol_table = symbol_table

    def _make_parser(self):
        return self.parser_factory(self.lexer_factory(self.symbol_table))

    def __call__(self, program):
        ast = self._make_parser().parse(program)
        return ast

compile = Compiler()


class Evaluator(object):

    compile = compile

    def __call__(self, program, *a, **kw):
        """Evaluates the program string or AST"""
        context = a and a[0] or kw
        ast = self.compile(program) if isinstance(program, string_types) else program
        return ast.evaluate(context)

e = Evaluator()


if __name__ == '__main__':
    tests = [
        ("T.user.is_staff AND T.user.is_admin",
         ('"user"."is_staff" AND "user"."is_admin"', [])),

        ("func.Lower(T.user.first_name) AND T.user.is_admin",
         ('LOWER("user"."first_name") AND "user"."is_admin"', [])),

        ("Concat(T.user.first_name, T.user.last_name) AND NOT (T.user.is_active AND (T.user.is_admin OR T.user.is_staff))",
         ('"user"."first_name" || "user"."last_name" AND NOT ("user"."is_active" AND ("user"."is_admin" OR "user"."is_staff"))', [])),

        ("T.user.age <@ func.int4range(25, 30)",
         ('"user"."age" <@ INT4RANGE(%s, %s)', [25, 30])),

        ("T.user.is_staff IS NOT TRUE AND T.user.is_admin IS FALSE",
         ('"user"."is_staff" IS NOT TRUE AND "user"."is_admin" IS FALSE', [])),

        ("T.user.is_staff AND T.user.age BETWEEN 25 AND 30 AND T.user.is_admin",
         ('"user"."is_staff" AND "user"."age" BETWEEN %s AND %s AND "user"."is_admin"', [25, 30])),

        ("T.user.is_staff AND T.user.age BETWEEN 25 AND 30 OR T.user.is_admin",
         ('"user"."is_staff" AND "user"."age" BETWEEN %s AND %s OR "user"."is_admin"', [25, 30])),

        ("T.user.is_staff AND T.user.age NOT BETWEEN 25 AND 30 + T.t1.delta AND T.t1.f1",
         ('"user"."is_staff" AND "user"."age" NOT BETWEEN %s AND (%s + "t1"."delta") AND "t1"."f1"', [25, 30])),

        ("T.user.age IN F.l1 AND T.user.age NOT IN F.l2')",
         ('"user"."age" IN "l1" AND "user"."age" NOT IN "l2"', [])),

        ("T.user.age DESC AND T.user.first_name ASC",
         ('"user"."age" DESC AND "user"."first_name" ASC', [])),

        ("T.user.age <@ func.int4range('%(min)s', '%(max)s')",
         ('"user"."age" <@ INT4RANGE(%s, %s)', ['%(min)s', '%(max)s']))
    ]

    for t, expected in tests:
        sql = smartsql.compile(e(t))
        print(t, '\n', sql, '\n\n')
        assert expected == sql
