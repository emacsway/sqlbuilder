# -*- coding: utf-8 -*-
import os
import sys


def main():
    import django
    from django.conf import settings
    settings.configure(
        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': ':memory:'
            }
        },
        INSTALLED_APPS = [
            'sqlbuilder.django_sqlbuilder',
        ],
        MIDDLEWARE_CLASSES = [
        ],
        STATIC_URL = '/static/',
        TEST_RUNNER = 'django.test.runner.DiscoverRunner',
        TEMPLATE_DIRS = [],
        DEBUG = True,
        TEMPLATE_DEBUG = True,
        ROOT_URLCONF = 'runtests',
    )

    from django.conf.urls import patterns, include, url
    global urlpatterns
    urlpatterns = patterns(
        ''
    )

    try:
        django.setup()
    except AttributeError:
        pass

    # Run the test suite, including the extra validation tests.
    from django.test.utils import get_runner
    TestRunner = get_runner(settings)

    test_runner = TestRunner(verbosity=1, interactive=False, failfast=False)
    failures = test_runner.run_tests([
        'sqlbuilder.django_sqlbuilder',
        'sqlbuilder.smartsql',
        'sqlbuilder.mini',
    ])
    sys.exit(failures)


if __name__ == "__main__":
    main()
