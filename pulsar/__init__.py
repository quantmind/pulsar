'''Concurrent server and message queues'''
VERSION = (0, 1, 0)

def get_version():
    return '.'.join(map(str,VERSION))

__version__   = get_version()
__license__   = "BSD"
__author__    = "Luca Sbardella"
__contact__   = "luca.sbardella@gmail.com"
__homepage__  = "https://github.com/quantmind/pulsar"
__docformat__ = "restructuredtext"
CLASSIFIERS  = [
                'Development Status :: 1 - Planning',
                'Environment :: Web Environment',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: BSD License',
                'Operating System :: OS Independent',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2.6',
                'Programming Language :: Python :: 2.7',
                'Programming Language :: Python :: 3',
                'Topic :: Utilities'
                ]

from .utils.log import *

DEFAULT_PORT = 8060
SERVER_SOFTWARE = "python-{0}/{1}".format(SERVER_NAME,get_version())

from .utils.exceptions import *
from .utils import system
from .utils.sock import *
from .utils.py2py3 import ispy3k, to_string, is_string
platform = system.platform

from .utils.config import *
from .async import *
from .workers.base import *
from .workers.monitor import *
from .apps.base import Application, require
from .apps import test


