#!/usr/bin/env python
import argparse
import sys
try:
    from xmlrpc.client import ServerProxy
except ImportError:
    from xmlrpclib import ServerProxy


from setup import meta


def main():
    parser = argparse.ArgumentParser(description='PyPI package checker')
    parser.add_argument(
        '--pypi-index-url',
        help='PyPI index URL.',
        default='https://pypi.python.org/pypi'
    )

    args = parser.parse_args()

    pypi = ServerProxy(args.pypi_index_url)
    releases = pypi.package_releases(meta['name'])

    if releases:
        print(next(iter(sorted(releases, reverse=True))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
