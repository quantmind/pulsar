'''Event driven concurrent framework for Python'''
VERSION = (0, 3, 0, 'final', 0)

from .utils.version import get_version

__version__   = version = get_version(VERSION)
__license__   = "BSD"
__author__    = "Luca Sbardella"
__contact__   = "luca.sbardella@gmail.com"
__homepage__  = "https://github.com/quantmind/pulsar"
__docformat__ = "restructuredtext"
CLASSIFIERS  = [
                'Development Status :: 3 - Alpha',
                'Environment :: Web Environment',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: BSD License',
                'Operating System :: OS Independent',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2.6',
                'Programming Language :: Python :: 2.7',
                'Programming Language :: Python :: 3',
                'Programming Language :: Python :: 3.1',
                'Programming Language :: Python :: 3.2',
                'Programming Language :: Python :: 3.3',
                'Topic :: Internet',
                'Topic :: Utilities',
                'Topic :: Software Development :: Libraries :: Python Modules',
                'Topic :: Internet :: WWW/HTTP',
                'Topic :: Internet :: WWW/HTTP :: WSGI',
                'Topic :: Internet :: WWW/HTTP :: WSGI :: Server',
                'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
                ]

from .utils.log import *

DEFAULT_PORT = 8060
SERVER_SOFTWARE = "python-{0}/{1}".format(SERVER_NAME, version)

class NOT_DONE(object):
    pass

class CLEAR_ERRORS(object):
    pass

from .utils.exceptions import *
from .utils.sock import *
from .utils import system
from .utils.py2py3 import ispy3k, to_string, is_string, native_str,\
                            to_bytestring
platform = system.platform

from .utils.config import *
from .async import *
from .apps import *



