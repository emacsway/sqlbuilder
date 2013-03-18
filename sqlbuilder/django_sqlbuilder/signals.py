from __future__ import absolute_import, unicode_literals
import django.dispatch

field_conversion = django.dispatch.Signal(providing_args=["result", "field", "model"])
