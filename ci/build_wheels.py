#!/usr/bin/env python
import os
import argparse
import logging

try:
    from xmlrpc.client import ServerProxy
except ImportError:
    from xmlrpclib import ServerProxy

import docker
# import tinys3

from setup import meta


ARCHITECTURES = ['x86_64', 'i686']
ML_IMAGE = "quay.io/pypa/manylinux1_%s"


LOGGER = logging.getLogger('wheels')
MODULE_PATH = os.path.abspath(os.path.curdir)


class InvalidVersion(Exception):
    pass


def parser():
    parser = argparse.ArgumentParser(
        description='Build wheels and upload them to s3'
    )
    parser.add_argument(
        '--pyversions',
        metavar='PYTHON-VERSIONS',
        nargs='+',
        help="Build wheels for these python versions"
    )
    parser.add_argument('--upgrade', metavar='PYPI-VERSION')
    parser.add_argument(
        '--release',
        action='store_true',
        help=(
            'Check if version is a valid release (alpha, beta, rc not allowed)'
        )
    )
    parser.add_argument(
        '--pypi-index-url',
        help='PyPI index URL.',
        default='https://pypi.python.org/pypi'
    )
    return parser


def pypi_release(pypi_index_url):
    pypi = ServerProxy(pypi_index_url)
    releases = pypi.package_releases(meta['name'])
    if releases:
        return next(iter(sorted(releases, reverse=True)))


def check_release(version):
    try:
        vtuple = list(map(int, version.split('.')))
        assert len(vtuple) == 3
    except Exception:
        raise InvalidVersion("Not a valid release version %s" % version)

    if vtuple <= parser.upgrade.split('.'):
        raise InvalidVersion("version %s not higher than %s" %
                             (version, parser.upgrade))


def ml_version(pyver):
    maj, min = pyver.split('.')
    return 'cp{maj}{min}-cp{maj}{min}m'.format(maj=maj, min=min)


def main(args=None):
    args = parser().parse_args(args)
    version = meta['version']
    if args.release:
        check_release(version)

    cli = docker.from_env(version='auto')

    LOGGER.info('Module path %s', MODULE_PATH)

    for arch in ARCHITECTURES:
        image = ML_IMAGE % arch
        for pyver in args.pyversions:
            LOGGER.info('')
            LOGGER.info(80*'=')
            LOGGER.info('Build wheels for python %s %s. Image %s',
                        pyver, arch, image)
            LOGGER.info(80 * '=')

            container = cli.containers.run(
                image,
                name="build_wheels_%s_%s" % (pyver, arch),
                command='/io/ci/build-manylinux-wheels.sh',
                volumes={MODULE_PATH: '/io'},
                environment=dict(
                    PYTHON_VERSION=ml_version(pyver),
                    PYMODULE=meta['name'],
                    CI='true'
                ),
                auto_remove=True,
                detach=True
            )
            for log in container.logs(stream=True):
                LOGGER.info(log.decode('utf-8').strip())


if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='%(message)s')
    try:
        print(main())
        exit(0)
    except InvalidVersion as exc:
        LOGGER.error(str(exc))
    except Exception:
        LOGGER.exception('Could not obtain valid semantic version')
    exit(1)
