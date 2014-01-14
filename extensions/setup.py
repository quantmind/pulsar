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
if sys.platform == 'win32' and sys.version_info > (2, 6):
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
            if "'path'" in str(sys.exc_info()[1]): # works with both py 2/3
                raise BuildFailed
            raise

lib_path = os.path.dirname(__file__)


def lua_extension():
    '''Create Lua extension module
    '''
    LUASKIP = ['lua.c', 'luac.c']
    src = []

    path = os.path.join(lib_path, 'lua', 'ext')
    include_dirs.append(path)
    for file in os.listdir(path):
        if file.endswith('.c'):
            src.append(os.path.join(path, file))

    path = os.path.join(lib_path, 'lua', 'src')
    include_dirs.append(path)
    for file in os.listdir(path):
        if file.endswith('.c') and file not in LUASKIP:
            src.append(os.path.join(path, file))

    src.append(os.path.join(lib_path, 'lua', 'lua.pyx'))
    #
    #extra_compile_args = ['-DLUA_ANSI']
    extra_compile_args = ['-DLUA_COMPAT_ALL']
    if sys.platform == 'darwin':
        extra_compile_args.append('-DLUA_USE_MACOSX')
    if os.name != 'posix':
        extra_compile_args.append('-DLUA_BUILD_AS_DLL')

    return Extension('pulsar.utils.lua',
                     src,
                     include_dirs=include_dirs,
                     extra_compile_args=extra_compile_args)


def lib_extension():
    path = os.path.join(lib_path, 'lib')
    include_dirs.append(path)
    return Extension('pulsar.utils.lib',
                     [os.path.join(path, 'lib.pyx')],
                     include_dirs=include_dirs)


extensions = [lib_extension(), lua_extension()]

libparams = {
             'ext_modules': cythonize(extensions),
             'cmdclass': {'build_ext' : tolerant_build_ext},
             'include_dirs': include_dirs
             }
