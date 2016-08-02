import sqlbuilder.smartsql as s
from sqlbuilder.smartsql import Infix

ADD = Infix(s.Add)
AND = Infix(s.And)
DIV = Infix(s.Div)
EQ = Infix(s.Eq)
GE = Infix(s.Ge)
GT = Infix(s.Gt)
LE = Infix(s.Le)
LSHFT = Infix(s.LShift)
LT = Infix(s.Lt)
MUL = Infix(s.Mul)
NE = Infix(s.Ne)
OR = Infix(s.Or)
RSHIFT = Infix(s.RShift)
SUB = Infix(s.Sub)

if __name__ == '__main__':
    t = s.T.t
    print(t.a |ADD| t.b |MUL| (t.c |SUB| t.d))
