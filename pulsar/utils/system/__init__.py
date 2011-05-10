import errno
from .runtime import Platform

platform = Platform()
seconds = platform.seconds

if platform.type == 'posix':
    from .posixsystem import *
elif platform.type == 'win':
    from .windowssystem import *

    