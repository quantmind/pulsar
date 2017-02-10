#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

make clean && make coverage

if [[ $COVERALLS == "yes" ]]; then
    python setup.py test --coveralls;
    cd docs && make spelling;
fi

python -W ignore setup.py test -q --io uv
python -W ignore setup.py test --http-py-parser -q
python setup.py bench
# - python setup.py bench --io uv
