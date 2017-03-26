from __future__ import absolute_import, unicode_literals
import django.dispatch

field_conversion = django.dispatch.Signal(providing_args=["result", "field", "model"])  # Deprecated
field_mangling = django.dispatch.Signal(providing_args=["field", "model"])
column_mangling = django.dispatch.Signal(providing_args=["column", "model"])
