# Based on idea of http://pyparsing.wikispaces.com/file/detail/simpleBool.py
# This module is analog to sqlbuilder.smartsql.contrib.infixes,
# but allows use operators in native SQL form, like @>, &>, -|- etc.
# Example of usage:
# >>> e("""T.user.is_staff and T.user.is_admin""")
# ... <And: "user"."is_staff" AND "user"."is_admin", []>
# This file is only draft, and still under construction!!!
# Don't use it in the production!!! It isn't ready yet!!!

from pyparsing import alphanums, delimitedList, infixNotation, opAssoc, Group, Keyword, Word, Suppress

import sqlbuilder.smartsql as s

TRUE = Keyword("True")
FALSE = Keyword("False")
variable = Word(alphanums + '._')
variable.setParseAction(lambda tokens: eval(tokens[0]))

parameters = Group(delimitedList(variable))
func_call = variable + Suppress("(") + parameters + Suppress(")")
func_call.setParseAction(lambda tokens: tokens[0](*tokens[1]))

operand = TRUE | FALSE | func_call | variable


def make_binary(op_factory, op_str=None):
    def _inner(tokens):
        args = tokens[0][0::2]
        if op_str:
            args.insert(1, op_str)
        return op_factory(*args)
    return _inner


def make_unary(op, op_str=None):
    def _inner(tokens):
        args = [tokens[0][1]]
        if op_str:
            args.insert(0, op_str)
        return op(*args)
    return _inner


def make_postfix(op, op_str=None):
    def _inner(tokens):
        args = [tokens[0][1]]
        if op_str:
            args.append(op_str)
        return op(*args)
    return _inner

# define expression, based on expression operand and
# list of operations in precedence order
boolExpr = infixNotation(operand, [
    # ("+", 1, opAssoc.RIGHT, make_unary(s.Pos)),
    # ("-", 1, opAssoc.RIGHT, make_unary(s.Neg)),
    # ("~", 1, opAssoc.RIGHT, make_unary(s.Unary, '~')),
    # ("*", 2, opAssoc.LEFT, make_binary(s.Mul)),
    # ("/", 2, opAssoc.LEFT, make_binary(s.Div)),
    # ("%", 2, opAssoc.LEFT, make_binary(s.Binary, '%')),
    # ("+", 2, opAssoc.LEFT, make_binary(s.Add)),
    # ("-", 2, opAssoc.LEFT, make_binary(s.Sub)),
    # ("<<", 2, opAssoc.LEFT, make_binary(s.LShift)),
    # (">>", 2, opAssoc.LEFT, make_binary(s.RShift)),
    # ("&", 2, opAssoc.LEFT, make_binary(s.Binary, '&')),
    # ("#", 2, opAssoc.LEFT, make_binary(s.Binary, '#')),
    ("|", 2, opAssoc.LEFT, make_binary(s.Binary, '|')),
    ("IS", 2, opAssoc.LEFT, make_binary(s.Is)),
    ("ISNULL", 1, opAssoc.LEFT, make_postfix(s.Postfix, 'ISNULL')),
    ("NOTNULL", 1, opAssoc.LEFT, make_postfix(s.Postfix, 'NOTNULL')),

    ("not", 1, opAssoc.RIGHT, make_unary(s.Not)),
    ("and", 2, opAssoc.LEFT,  make_binary(s.And)),
    ("or",  2, opAssoc.LEFT,  make_binary(s.Or)),
])


def eval_expr(expr_str):
    return boolExpr.parseString(expr_str)[0]

e = eval_expr

if __name__ == "__main__":
    T = s.T
    p = T.user.p
    q = T.user.q
    r = T.user.r
    tests = [
        ("p", ("", [])),
        ("q", ("", [])),
        ("T.user.p and q", ("", [])),
        ("p and not q", ("", [])),
        ("not not p", ("", [])),
        ("not(p and q)", ("", [])),
        ("q or not p and r", ("", [])),
        ("q or not p or not r", ("", [])),
        ("q or not (p and r)", ("", [])),
        ("p or q or r", ("", [])),
        ("p or q or r and False", ("", [])),
        ("(p or q or r) and False", ("", [])),
        ("T.user.is_staff and T.user.is_admin", ("", [])),
        ("s.Lower(T.user.first_name) and T.user.is_admin", ("", [])),
        ("s.Concat(T.user.first_name, T.user.last_name) and T.user.is_admin", ("", [])),
    ]

    print("p =", p)
    print("q =", q)
    print("r =", r)
    print()
    for t, expected in tests:
        res = eval_expr(t)
        print(t, '\n', res, s.compile(res), '\n\n')
