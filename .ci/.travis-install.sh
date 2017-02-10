#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    # https://github.com/travis-ci/travis-ci/issues/6307#issuecomment-233315824
    rvm get head

    git clone --depth 1 https://github.com/yyuu/pyenv.git ~/.pyenv
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv global ${PYTHON_VERSION}
    pyenv rehash

    brew update

    brew install gnu-sed --with-default-names
    brew outdated libtool || brew upgrade libtool
    brew outdated autoconf || brew upgrade autoconf --with-default-names
    brew outdated automake || brew upgrade automake --with-default-names
fi

if [ "${TRAVIS_OS_NAME}" == "linux" ]; then
    sudo apt-get install enchant
fi

./install-dev.sh
