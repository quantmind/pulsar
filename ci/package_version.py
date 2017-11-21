#!/usr/bin/env python
import argparse
import logging
from setup import meta


LOGGER = logging.getLogger('pulsar_test')


class InvalidVersion(Exception):
    pass


def parser():
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description='Check package version')
    parser.add_argument('--upgrade', metavar='PYPI-VERSION')
    parser.add_argument(
        '--release',
        action='store_true',
        help=(
            'Check if version is a valid release (alpha, beta, rc not allowed)'
        )
    )
    return parser


def main(args=None):
    args = parser().parse_args(args)
    version = meta['version']
    if args.release:
        try:
            vtuple = list(map(int, version.split('.')))
            assert len(vtuple) == 3
        except Exception:
            raise InvalidVersion("Not a valid release version %s" % version)

        if vtuple <= parser.upgrade.split('.'):
            raise InvalidVersion("version %s not higher than %s" %
                                 (version, parser.upgrade))

    return version


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
