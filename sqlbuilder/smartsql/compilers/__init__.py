import os
import sys
from sqlbuilder.smartsql import warn

warn('sqlbuilder.smartsql.compilers', 'sqlbuilder.smartsql.dialects')
__path__.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(sys.modules[__name__].__file__))),
    'dialects'
))
