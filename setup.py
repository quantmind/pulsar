#!/usr/bin/env python
import os

from setuptools import setup, find_packages

from extensions import ext

from pulsar import cmds
import pulsar


class PulsarTest(cmds.Test):
    start_coverage = True


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


meta = dict(
    name='pulsar',
    version=pulsar.__version__,
    description=pulsar.__doc__,
    author=pulsar.__author__,
    author_email="luca@quantmind.com",
    maintainer_email="luca@quantmind.com",
    url="https://github.com/quantmind/pulsar",
    license="BSD",
    long_description=read('README.rst'),
    include_package_data=True,
    install_requires=requirements('requirements/hard.txt')[0],
    setup_requires=['wheel'],
    packages=find_packages(include=['pulsar', 'pulsar.*']),
    entry_points={
        "distutils.commands": [
            "pulsar_test = pulsar.cmds:Test",
            "bench = pulsar.cmds:Bench",
            "linux_wheels = pulsar.cmds:ManyLinux",
            "s3_upload = pulsar.cmds:S3Data"
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


def run_setup(with_cext):
    params = ext.params() if with_cext else {}
    params.update(meta)
    cmdclass = params.get('cmdclass', {})
    cmdclass['test'] = PulsarTest
    cmdclass['bench'] = cmds.Bench
    cmdclass['linux_wheels'] = cmds.ManyLinux
    cmdclass['s3data'] = cmds.S3Data
    cmdclass['pypi'] = cmds.PyPi
    params['cmdclass'] = cmdclass
    setup(**params)


if __name__ == '__main__':
    err = None
    try:
        run_setup(True)
    except ext.BuildFailed as exc:
        err = exc.msg
    if err:
        print('WARNING: C extensions could not be compiled: %s' % err)
        run_setup(False)
