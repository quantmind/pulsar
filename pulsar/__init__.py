# -*- coding: utf-8 -
"""Event driven concurrent framework for Python"""
import os

from .utils.version import get_version


VERSION = (1, 3, 1, 'final', 0)

__version__ = version = get_version(VERSION)


DEFAULT_PORT = 8060
ASYNC_TIMEOUT = None
SERVER_NAME = 'pulsar'
JAPANESE = b'\xe3\x83\x91\xe3\x83\xab\xe3\x82\xb5\xe3\x83\xbc'.decode('utf-8')
CHINESE = b'\xe8\x84\x89\xe5\x86\xb2\xe6\x98\x9f'.decode('utf-8')
HINDI = (b'\xe0\xa4\xaa\xe0\xa4\xb2\xe0\xa5\x8d'
         b'\xe0\xa4\xb8\xe0\xa4\xb0').decode('utf-8')
SERVER_SOFTWARE = "{0}/{1}".format(SERVER_NAME, version)

if os.environ.get('pulsar_speedup') == 'no':
    HAS_C_EXTENSIONS = False
else:
    HAS_C_EXTENSIONS = True
    try:
        from .utils import lib      # noqa
    except ImportError:
        HAS_C_EXTENSIONS = False

from .utils.exceptions import *     # noqa
from .utils import system           # noqa
platform = system.platform          # noqa
from .utils.config import *         # noqa
from .async import *                # noqa
from .apps import *                 # noqa
from .apps.data import data_stores  # noqa
