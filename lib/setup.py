#
# Required by Cython to build extensions
import os

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

SRC_DIR = os.path.join(os.path.split(os.path.abspath(__file__))[0],'src')

ext_modules  = Extension('pulsar.http.cparser.parser', ['lib/src/http_parser.c',
                                                        'lib/src/parser.pyx'])

libparams = {
             'ext_modules': [ext_modules],
             'cmdclass': {'build_ext' : build_ext},
             'include_dirs': [SRC_DIR]
             }
