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
    
try:
    import numpy
    include_dirs = [numpy.get_include()]
except ImportError:
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

################################################################################
##    EXTENSIONS
lib_path = os.path.dirname(__file__)
extra_compile_args = []
#if sys.platform == 'darwin':
#    #extra_compile_args.extend(('-std=c++11', '-stdlib=libc++'))
#    extra_compile_args.extend(('-std=c++11', '-stdlib=libc++'))
#    #extra_compile_args.extend(('-std=c++11',))
#elif os.name != 'nt':
#    extra_compile_args.append('-std=gnu++0x')
    

extension = Extension('pulsar.apps.redis.cparser',
                      [os.path.join(lib_path, 'src', 'cparser.pyx')],
                      language='c++',
                      #extra_compile_args=extra_compile_args,
                      include_dirs=include_dirs)

include_dirs.append(os.path.join(lib_path, 'src'))


libparams = {
             'ext_modules': cythonize(extension),
             'cmdclass': {'build_ext' : tolerant_build_ext},
             'include_dirs': include_dirs
             }
