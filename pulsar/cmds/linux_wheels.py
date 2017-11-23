#!/usr/bin/env python
"""Build python wheels for a package

Optionally check if the version is greatre then the current version on PyPI
(pass the --release flag in the command line)
"""
import os
from distutils.cmd import Command
from distutils.errors import DistutilsModuleError

try:
    import docker
except ImportError:
    docker = None


ARCHITECTURES = ['x86_64', 'i686']
ML_IMAGE = "quay.io/pypa/manylinux1_%s"

WHEELS_SH = os.path.join(os.path.dirname(__file__), 'build-wheels.sh')
MODULE_PATH = os.path.abspath(os.path.curdir)
MODULE_PATH_DOCKER = '/io'


def ml_version(pyver):
    maj, min = pyver.split('.')
    return 'cp{maj}{min}-cp{maj}{min}m'.format(maj=maj, min=min)


class ManyLinux(Command):
    description = 'Build wheels for several linux platforms'

    user_options = [
        ('py=', None,
         "comma separated list of python versions for which wheels"
         " are required (MAJOIR.MINOR)"),
        ('build-base=', 'b',
         "base directory for build library"),
    ]

    def initialize_options(self):
        self.build_base = 'build'
        self.py = None

    def finalize_options(self):
        versions = [s.strip() for s in (self.py or '').split(',')]
        self.py = versions

    def run(self):
        if not docker:
            raise DistutilsModuleError('linux_wheels requires docker package')

        meta = self.distribution.metadata
        version = meta.version
        self.announce('Building many linux wheels for version %s' % version)

        cli = docker.from_env(version='auto')

        self.announce('Module path %s' % MODULE_PATH)

        pkg_dir = os.path.abspath(self.distribution.package_dir or os.curdir)
        target = os.path.join(self.build_base, 'build-wheels.sh')
        command = '%s/%s' % (MODULE_PATH_DOCKER, target)

        for arch in ARCHITECTURES:
            image = ML_IMAGE % arch
            for pyver in self.py:
                self.announce('', 2)
                self.announce(80*'=', 2)
                self.announce(
                    'Build wheels for python %s %s. Image %s' %
                    (pyver, arch, image),
                    3
                )
                self.announce(80 * '=', 2)

                target_file = os.path.join(pkg_dir, target)
                target_dir = os.path.dirname(target_file)
                if not os.path.isdir(target_dir):
                    os.makedirs(target_dir)
                self.copy_file(WHEELS_SH, os.path.join(pkg_dir, target))
                whl = ml_version(pyver)

                container = cli.containers.run(
                    image,
                    name="build_wheels_%s_%s" % (pyver, arch),
                    command=command,
                    volumes={MODULE_PATH: MODULE_PATH_DOCKER},
                    environment=dict(
                        PYTHON_VERSION=whl,
                        PYMODULE=meta.name,
                        WHEEL='manylinux1',
                        BUNDLE_WHEEL='auditwheel',
                        IOPATH=MODULE_PATH_DOCKER,
                        PYTHON="/opt/python/%s/bin/python" % whl,
                        PIP="/opt/python/%s/bin/pip" % whl,
                        CI="true"
                    ),
                    auto_remove=True,
                    detach=True
                )
                for log in container.logs(stream=True):
                    self.announce(log.decode('utf-8').strip(), 2)
