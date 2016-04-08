#
# Required by Cython to build Hiredis extensions
#
import os
import sys
from distutils.extension import Extension
from distutils.command.build_ext import build_ext
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)

path = os.path.join('extensions', 'lib')
ext_file = os.path.join(path, 'lib.c')
ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

if sys.platform == 'win32':
    # 2.6's distutils.msvc9compiler can raise an IOError when failing to
    # find the compiler
    ext_errors += (IOError,)


class BuildFailed(Exception):

    def __init__(self, msg=None):
        if not msg:
            msg = str(sys.exc_info()[1])
        self.msg = msg


class tolerant_build_ext(build_ext):
    # This class allows C extension building to fail. From SQLAlchemy

    def run(self):
        try:
            if not os.path.isfile(ext_file):
                try:
                    from Cython.Build import cythonize
                except ImportError:
                    raise BuildFailed('Cython not installed') from None
                self.extensions = cythonize(self.extensions,
                                            include_path=[path])
            super().run()
        except DistutilsPlatformError:
            raise BuildFailed from None

    def build_extension(self, ext):
        try:
            super().build_extension(ext)
        except ext_errors:
            raise BuildFailed from None
        except ValueError:
            # this can happen on Windows 64 bit, see Python issue 7511
            if "'path'" in str(sys.exc_info()[1]):  # works with both py 2/3
                raise BuildFailed from None
            raise


def params(cython=False):
    if not cython:
        cython = not os.path.isfile(ext_file)

    if cython and os.path.isfile(ext_file):
        os.remove(ext_file)

    file_name = 'lib.pyx' if cython else 'lib.c'

    extension = Extension('pulsar.utils.lib',
                          [os.path.join(path, file_name)],
                          include_dirs=[path])

    extensions = [extension]

    return {'ext_modules': extensions,
            'cmdclass': {'build_ext': tolerant_build_ext},
            'include_dirs': [path]}
