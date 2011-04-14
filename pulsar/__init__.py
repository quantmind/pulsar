'''Concurrent server and message queues'''

VERSION = (0, 1, 'dev')


def get_version():
    return '.'.join(map(str,VERSION))

SERVER_NAME = "pulsar"
SERVER_SOFTWARE = "{0}/{1}".format(SERVER_NAME,get_version())


def getLogger(name = None):
    import logging
    name = '{0}.{1}'.format(SERVER_NAME,name) if name else SERVER_NAME
    return logging.getLogger(name) 

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

from .utils.exceptions import *
from .utils import test
from .utils import system
from .utils.py2py3 import ispy3k, to_string, is_string
platform = system.platform

from .utils.config import *
from .workers.workerpool import *
from .workers.base import *
from .workers.arbiter import *
from .apps.base import Application, require


