'''Operative system specific functions and classes
'''
from .runtime import Platform

platform = Platform()
seconds = platform.seconds

from .base import *

if platform.type == 'posix':
    from .posixsystem import *
elif platform.type == 'win':
    from .windowssystem import *

    