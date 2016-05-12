#!/usr/bin/env python
import sys
import os
import json
import subprocess

from setuptools import setup, find_packages
from extensions import ext

import pulsar_test


class PulsarTest(pulsar_test.Test):
    start_coverage = True


def extend(params, package=None):
    if package:
        path = os.path.dirname(__file__)
        data = sh('%s %s package_info %s %s'
                  % (sys.executable, __file__, package, path))
        params.update(json.loads(data))

    return params


def read(name):
    filename = os.path.join(os.path.dirname(__file__), name)
    with open(filename) as fp:
        return fp.read()


def requirements(name):
    install_requires = []
    dependency_links = []

    for line in read(name).split('\n'):
        if line.startswith('-e '):
            link = line[3:].strip()
            if link == '.':
                continue
            dependency_links.append(link)
            line = link.split('=')[1]
        line = line.strip()
        if line:
            install_requires.append(line)

    return install_requires, dependency_links


def sh(command):
    return subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True).communicate()[0]


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
    packages=find_packages(include=['pulsar', 'pulsar.*', 'pulsar_test']),
    entry_points={
        "distutils.commands": [
            "pulsar_test = pulsar_test:Test"
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
        'Topic :: Internet',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: WSGI',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Server',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content']
)


def run_setup(with_cext):
    params = ext.params() if with_cext else {}
    params.update(meta)
    cmdclass = params.get('cmdclass', {})
    cmdclass['test'] = PulsarTest
    params['cmdclass'] = cmdclass
    setup(**extend(params, 'pulsar'))


def package_info():
    package = sys.argv[2]
    if len(sys.argv) > 3:
        sys.path.append(sys.argv[3])
    os.environ['package_info'] = package
    pkg = __import__(package)
    print(json.dumps(dict(version=pkg.__version__,
                          description=pkg.__doc__)))


if __name__ == '__main__':
    command = sys.argv[1] if len(sys.argv) > 1 else None
    if command == 'package_info':
        package_info()
    elif command == 'agile':
        from agile.app import AgileManager
        AgileManager(description='Release manager for pulsar',
                     argv=sys.argv[2:]).start()
    else:
        try:
            run_setup(True)
        except ext.BuildFailed as exc:
            print('WARNING: C extensions could not be compiled: %s' % exc.msg)
            run_setup(False)
