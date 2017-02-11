#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

make clean && make testall

if [ "${COVERALLS}" == "yes" ]; then
    python setup.py test --coveralls;
    cd docs && make spelling;
fi
