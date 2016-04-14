#!/usr/bin/env python
from multiprocessing import current_process

import setuptools.command.test as orig
from setuptools import setup, find_packages
from extensions import ext

import pulsar_config as config


meta = dict(
    name='pulsar',
    author="Luca Sbardella",
    author_email="luca@quantmind.com",
    maintainer_email="luca@quantmind.com",
    url="https://github.com/quantmind/pulsar",
    license="BSD",
    long_description=config.read('README.rst'),
    include_package_data=True,
    setup_requires=['wheel'],
    packages=find_packages(include=['pulsar', 'pulsar.*']),
    entry_points={
        "distutils.commands": [
            "pulsar_test = pulsar.apps.test.setup:Test"
        ]
    },
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


class PulsarTest(orig.test):
    test_suite = True

    @property
    def testcls(self):
        from pulsar.apps.test.setup import Test
        return Test

    @property
    def user_options(self):
        return self.testcls.user_options

    def initialize_options(self):
        self.testcls.initialize_options(self)

    def finalize_options(self):
        self.testcls.initialize_options(self)

    def run_tests(self):
        if self.coverage:
            import coverage
            from coverage.monkey import patch_multiprocessing
            print('Collect coverage')
            p = current_process()
            p._coverage = coverage.Coverage(data_suffix=True)
            patch_multiprocessing()
            p._coverage.start()
        self.testcls.run_tests(self)


def run_setup(with_cext):
    params = ext.params() if with_cext else {}
    params.update(meta)
    cmdclass = params.get('cmdclass', {})
    cmdclass['test'] = PulsarTest
    params['cmdclass'] = cmdclass
    setup(**config.setup(params, 'pulsar'))


if __name__ == '__main__':
    try:
        run_setup(True)
    except ext.BuildFailed as exc:
        print('WARNING: C extensions could not be compiled: %s' % exc.msg)
        run_setup(False)
