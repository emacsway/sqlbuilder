# Based on idea of http://pyparsing.wikispaces.com/file/detail/simpleBool.py
# Example of usage:
# >>> e("""T.user.is_staff and T.user.is_admin""")
# ... <And: "user"."is_staff" AND "user"."is_admin", []>
# Created especially for postgresql operators like @>, &>, -|- etc.
# This file only draft, under construction. Don't use it in production. It is not ready yet.

from pyparsing import infixNotation, opAssoc, Keyword, Word, alphanums

import sqlbuilder.smartsql as s

TRUE = Keyword("True")
FALSE = Keyword("False")
boolOperand = TRUE | FALSE | Word(alphanums + '._')
boolOperand.setParseAction(lambda tokens: eval(tokens[0]))


def make_binary(op, op_str=None):
    def _inner(tokens):
        args = tokens[0][0::2]
        if op_str:
            args.insert(1, op_str)
        return op(*args)
    return _inner


def make_unary(op):
    def _inner(tokens):
        return op(tokens[0][1])
    return _inner

# define expression, based on expression operand and
# list of operations in precedence order
boolExpr = infixNotation(boolOperand, [
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
        ("T.user.is_staff and T.user.is_admin", ("", []))
    ]

    print("p =", p)
    print("q =", q)
    print("r =", r)
    print()
    for t, expected in tests:
        res = eval_expr(t)
        print(t, '\n', res, s.compile(res), '\n\n')
