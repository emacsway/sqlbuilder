import os
import sys
import warnings

warnings.warn("sqlbuilder.django_sqlbuilder is deprecated. Use django_sqlbuilder instead", PendingDeprecationWarning, stacklevel=2)
__path__.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(sys.modules[__name__].__file__)))),
    'django_sqlbuilder'
))
