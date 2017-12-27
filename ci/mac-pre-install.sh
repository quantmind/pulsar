#!/bin/bash

set -e -x

brew update

brew upgrade pyenv
brew install gnu-sed --with-default-names
brew outdated libtool || brew upgrade libtool
brew outdated autoconf || brew upgrade autoconf --with-default-names
brew outdated automake || brew upgrade automake --with-default-names


if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
    pyenv install ${PYTHON_VERSION}
fi
pyenv global ${PYTHON_VERSION}
pyenv rehash

pyenv exec pip install virtualenv
pyenv exec virtualenv venv
