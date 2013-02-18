#!/usr/bin/env python
'''Python script for linkin the ``pulsar`` directory to your python distribution package directory
'''
import sys
import os
from distutils.sysconfig import get_python_lib
from pulsar.utils import filesystem


if __name__ == '__main__':
    plib = os.path.join(get_python_lib(),'pulsar')
    fname = os.path.join(filesystem.filedir(__file__),'pulsar')
    os.symlink(fname, plib)