#!/usr/bin/env python
#
# Copyright (c) 2011-2013 Ivan Zakrevsky and contributors.
import os.path
from setuptools import setup, find_packages

app_name = os.path.basename(os.path.dirname(os.path.abspath(__file__)))

setup(
    name = app_name,
    version = '0.7.10.11',

    packages = find_packages(),
    include_package_data=True,

    author = "Ivan Zakrevsky and contributors",
    author_email = "ivzak@yandex.ru",
    description = "SmartSQL - lightweight sql builder.",
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    license = "BSD License",
    keywords = "SQL database",
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite = 'runtests.main',
    tests_require = [
        'Django>=1.8',
    ],
    url = "https://bitbucket.org/emacsway/{0}".format(app_name),
)
