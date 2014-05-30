import os
import signal
import subprocess

from .base import *

__all__ = ['close_on_exec',
           'Waker',
           'daemonize',
           'socketpair',
           'EXIT_SIGNALS',
           'get_uid',
           'get_gid',
           'get_maxfd',
           'set_owner_process',
           'current_process']

# standard signal quit
EXIT_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT, signal.SIGQUIT)
# Default maximum for the number of available file descriptors.
REDIRECT_TO = getattr(os, "devnull", "/dev/null")

socketpair = None


def get_parent_id():
    return os.getppid()


def chown(path, uid, gid):
    pass


def close_on_exec(fd):
    pass


def get_uid(user=None):
    if not user:
        return os.geteuid()
    elif user.isdigit() or isinstance(user, int):
        return int(user)
    else:
        return pwd.getpwnam(user).pw_uid


def get_gid(group=None):
    if not group:
        return os.getegid()
    elif group.isdigit() or isinstance(group, int):
        return int(group)
    else:
        return grp.getgrnam(group).gr_gid


def set_owner_process(uid, gid):
    pass


def setpgrp():
    os.setpgrp()


def get_maxfd():
    return MAXFD


def daemonize():    # pragma    nocover
    pass


class MockProcess:
    pass


_process = MockProcess()


def current_process():
    return _process


class Waker(object):

    def fileno(self):
        pass

    def wake(self):
        pass
