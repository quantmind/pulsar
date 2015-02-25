#
# Required by Cython to build Hiredis extensions
#
import os
import sys
from distutils.core import setup
from distutils.extension import Extension
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)

from Cython.Distutils import build_ext
from Cython.Build import cythonize

include_dirs = []
ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


class BuildFailed(Exception):

    def __init__(self, msg=None):
        if not msg:
            msg = str(sys.exc_info()[1])
        self.msg = msg


class tolerant_build_ext(build_ext):
    # This class allows C extension building to fail. From SQLAlchemy

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed
        except ValueError:
            # this can happen on Windows 64 bit, see Python issue 7511
            if "'path'" in str(sys.exc_info()[1]):  # works with both py 2/3
                raise BuildFailed
            raise

lib_path = 'extensions'


def lib_extension():
    path = os.path.join(lib_path, 'lib')
    include_dirs.append(path)
    return Extension('pulsar.utils.lib',
                     [os.path.join(path, 'lib.pyx')],
                     include_dirs=include_dirs)


def libparams():
    extensions = [lib_extension()]
    return {'ext_modules': cythonize(extensions),
            'cmdclass': {'build_ext': tolerant_build_ext},
            'include_dirs': include_dirs}
