__all__ = ('CONTEXT', 'DEFAULT_DIALECT', 'LOOKUP_SEP', 'MAX_PRECEDENCE', 'OPERATOR', 'PLACEHOLDER',)


LOOKUP_SEP = '__'
MAX_PRECEDENCE = 1000
DEFAULT_DIALECT = 'postgres'
PLACEHOLDER = "%s"  # Can be re-defined by registered dialect.


class CONTEXT:
    QUERY = 'QUERY'
    FIELD = 'FIELD'
    FIELD_PREFIX = 'FIELD_PREFIX'
    FIELD_NAME = 'FIELD_NAME'
    TABLE = 'TABLE'
    EXPR = 'EXPR'
    SELECT = 'SELECT'

class OPERATOR:
    ADD = '+'
    SUB = '-'
    MUL = '*'
    DIV = '/'
    GT = '>'
    LT = '<'
    GE = '>='
    LE = '<='
    AND = 'AND'
    OR = 'OR'
    EQ = '='
    NE = '<>'
    IS = 'IS'
    IS_NOT = 'IS NOT'
    IN = 'IN'
    NOT_IN = 'NOT IN'
    RSHIFT = '>>'
    LSHIFT = '<<'
    LIKE = 'LIKE'
    ILIKE = 'ILIKE'
