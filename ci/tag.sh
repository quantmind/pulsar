#!/usr/bin/env bash

VERSION="$(python setup.py --version)"
echo ${VERSION}

git config --global user.email "bot@quantmind.com"
git config --global user.username "qmbot"
git config --global user.name "Quantmind Bot"
git push
git tag -am "Release $VERSION [ci skip]" ${VERSION}
git push --tags
