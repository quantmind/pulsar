#!/usr/bin/env bash

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r requirements/ci.txt
pip install -r requirements/test-posix.txt
pip install -r requirements/hard.txt
