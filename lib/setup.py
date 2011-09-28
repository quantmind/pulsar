#
# Required by Cython to build extensions
import os

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext


ext_modules  = Extension('pulsar.lib._pulsar', ['lib/http-parser/http_parser.c',
                                                'lib/src/parser.pyx'])


base_path = os.path.split(os.path.abspath(__file__))[0]
include_dirs = [os.path.join(base_path,'http-parser'),
                os.path.join(base_path,'src')]

libparams = {
             'ext_modules': [ext_modules],
             'cmdclass': {'build_ext' : build_ext},
             'include_dirs': include_dirs
             }