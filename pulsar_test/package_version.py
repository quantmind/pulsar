#!/usr/bin/env python3
import argparse
import logging


LOGGER = logging.getLogger('pulsar_test')


class InvalidVersion(Exception):
    pass


def main():
    parser = argparse.ArgumentParser(description='Check package version')
    parser.add_argument('package_name', metavar='PACKAGE-NAME')
    parser.add_argument('--upgrade', metavar='PYPI-VERSION')

    args = parser.parse_args()

    package = __import__(args.package_name)
    version = package.__version__
    try:
        vtuple = list(map(int, version.split('.')))
        assert len(vtuple) == 3, "Not a release version %s" % version
    except Exception:
        raise InvalidVersion("Not a valid release version %s" % version)

    if parser.upgrade:
        if vtuple <= parser.upgrade.split('.'):
            raise InvalidVersion("version %s not higher than %s" %
                                 (version, parser.upgrade))

    return '.'.join(vtuple)


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
