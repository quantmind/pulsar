#!/usr/bin/env python
import os
from setuptools import setup, find_packages
from extensions import utils


# Try to import lib build
try:
    from extensions.setup import libparams
except ImportError:
    libparams = None


def read(name):
    with open(name) as fp:
        return fp.read()


meta = dict(
    name='pulsar',
    author="Luca Sbardella",
    author_email="luca@quantmind.com",
    maintainer_email="luca@quantmind.com",
    url="https://github.com/quantmind/pulsar",
    license="BSD",
    long_description=read('README.rst'),
    include_package_data=True,
    setup_requires=['wheel'],
    packages=find_packages(exclude=['tests.*',
                                    'tests',
                                    'examples',
                                    'examples.*',
                                    'extensions',
                                    'extensions.*']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: WSGI',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Server',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content']
)


def run_setup():
    if libparams is None:
        params = {}
        print('WARNING: C extensions could not be included, '
              'cython is not installed.')
    else:
        params = libparams()
    params.update(meta)
    utils.extend(params, 'pulsar')
    setup(**params)


if __name__ == '__main__':
    run_setup()
