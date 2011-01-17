import os

if os.name == 'posix':
    from .posixsystem import *
elif os.name == 'nt':
    from .windowssystem import *
    