from pulsar.utils.system import platform

if platform.type == 'posix':
    from .posix import *
elif platform.type == 'win':
    from .win import *