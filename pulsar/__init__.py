# -*- coding: utf-8 -
'''Event driven concurrent framework for Python'''
VERSION = (0, 6, 0, 'final', 2)

from .utils.version import get_version

__version__   = version = get_version(VERSION)
__license__   = "BSD"
__author__    = "Luca Sbardella"
__contact__   = "luca.sbardella@gmail.com"
__homepage__  = "https://github.com/quantmind/pulsar"
__docformat__ = "restructuredtext"
CLASSIFIERS  = ['Development Status :: 4 - Beta',
                'Environment :: Web Environment',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: BSD License',
                'Operating System :: OS Independent',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2',
                'Programming Language :: Python :: 2.6',
                'Programming Language :: Python :: 2.7',
                'Programming Language :: Python :: 3',
                'Programming Language :: Python :: 3.2',
                'Programming Language :: Python :: 3.3',
                'Programming Language :: Python :: Implementation :: PyPy',
                'Topic :: Internet',
                'Topic :: Utilities',
                'Topic :: System :: Distributed Computing',
                'Topic :: Software Development :: Libraries :: Python Modules',
                'Topic :: Internet :: WWW/HTTP',
                'Topic :: Internet :: WWW/HTTP :: WSGI',
                'Topic :: Internet :: WWW/HTTP :: WSGI :: Server',
                'Topic :: Internet :: WWW/HTTP :: Dynamic Content']


DEFAULT_PORT = 8060
ASYNC_TIMEOUT = None
SERVER_NAME = 'Pulsar'
JAPANESE = b'\xe3\x83\x91\xe3\x83\xab\xe3\x82\xb5\xe3\x83\xbc'.decode('utf-8')
CHINESE = b'\xe8\x84\x89\xe5\x86\xb2\xe6\x98\x9f'.decode('utf-8')
SERVER_SOFTWARE = "python-{0}/{1}".format(SERVER_NAME, version)

from .utils.exceptions import *
from .utils import system
from .utils.internet import parse_address
from .utils.pep import to_string, native_str, to_bytes
platform = system.platform

from .utils.config import *
from .async import *
from .apps import *
#
# Import pubsub local backend for commands
from .apps.pubsub import local
del local
# Import tasks local backend for commands
from .apps.tasks.backends import local
del local
