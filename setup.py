#!/usr/bin/env python

import os
from setuptools import setup


VERSION = '1.0'


def read(fname):
    """ Return content of specified file """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='squery-pg',
    version=VERSION,
    license='BSD',
    packages=['squery_pg'],
    include_package_data=True,
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries',
    ],
    install_requires=[
        'psycopg2',
        'gevent',
        'sqlize-pg',
    ],
)
