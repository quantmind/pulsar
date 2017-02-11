#!/usr/bin/env bash

set -e -x


if [ -z "${TRAVIS_TAG}" ]; then
    # Not a release
    exit 0
fi


PACKAGE_VERSION=$(python "pulsar_test/package_version.py")
PYPI_VERSION=$(python "pulsar_test/pypi_check.py" "${PYMODULE}")
