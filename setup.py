#!/usr/bin/env python
import os
from setuptools import setup, find_packages

os.environ['pulsar_setup_running'] = 'yes'

package_name = 'pulsar'
package_fullname = package_name

# Try to import lib build
try:
    from extensions.setup import libparams
except ImportError:
    libparams = None

mod = __import__(package_name)


def read(name):
    with open(name) as fp:
        return fp.read()


def run_setup():
    if libparams is None:
        params = {}
        print('WARNING: C extensions could not be compiled, '
              'Maybe Cython is not installed.')
    else:
        params = libparams()

    params.update(dict(name=package_fullname,
                       version=mod.__version__,
                       author=mod.__author__,
                       author_email=mod.__contact__,
                       maintainer_email=mod.__contact__,
                       url=mod.__homepage__,
                       license=mod.__license__,
                       description=mod.__doc__,
                       long_description=read('README.rst'),
                       include_package_data=True,
                       packages=find_packages(exclude=['tests.*',
                                                       'tests',
                                                       'examples',
                                                       'examples.*']),
                       setup_requires=['wheel'],
                       classifiers=mod.CLASSIFIERS))
    setup(**params)


if __name__ == '__main__':
    run_setup()
