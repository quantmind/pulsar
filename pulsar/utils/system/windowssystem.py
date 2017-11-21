import os
import signal
import ctypes
import ctypes.wintypes
import getpass
from multiprocessing import current_process

from .base import *     # noqa

__all__ = ['daemonize',
           'EXIT_SIGNALS',
           'SIGNALS',
           'kill',
           'get_uid',
           'get_gid',
           'get_maxfd',
           'set_owner_process',
           'current_process']

# See: http://msdn.microsoft.com/en-us/library/ms724935(VS.85).aspx
SetHandleInformation = ctypes.windll.kernel32.SetHandleInformation
SetHandleInformation.argtypes = (ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD,
                                 ctypes.wintypes.DWORD)
SetHandleInformation.restype = ctypes.wintypes.BOOL

MAXFD = 1024
HANDLE_FLAG_INHERIT = 0x00000001
EXIT_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT, signal.SIGBREAK)
SIGNALS = ()


def kill(pid, sig):
    os.kill(pid, sig)


def set_owner_process(gid, uid):
    return None


def get_parent_id():
    try:
        return os.getppid()
    except Exception:
        return None


def chown(path, uid, gid):
    pass


def _set_non_blocking(fd):
    pass


def get_uid(user=None):
    if not user:
        return getpass.getuser()
    elif user == getpass.getuser():
        return user


def get_gid(group=None):
    return None


def setpgrp():
    pass


def get_maxfd():
    return MAXFD


def daemonize():
    pass
