import os
import fcntl
import resource
import grp
import pwd
import signal
import socket
import ctypes
from multiprocessing import Pipe, current_process

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

socketpair = socket.socketpair


def get_parent_id():
    return os.getppid()


def chown(path, uid, gid):
    try:
        os.chown(path, uid, gid)
    except OverflowError:
        os.chown(path, uid, -ctypes.c_int(-gid).value)


def close_on_exec(fd):
    if fd:
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def _set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


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
    """ set user and group of workers processes """
    if gid:
        try:
            os.setgid(gid)
        except OverflowError:
            # versions of python < 2.6.2 don't manage unsigned int for
            # groups like on osx or fedora
            os.setgid(-ctypes.c_int(-gid).value)
    if uid:
        os.setuid(uid)


def setpgrp():
    os.setpgrp()


def get_maxfd():
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    return maxfd


def daemonize():    # pragma    nocover
    """Standard daemonization of a process. Code is based on the
ActiveState recipe at http://code.activestate.com/recipes/278731/"""
    if os.fork() == 0:
        os.setsid()
        if os.fork() != 0:
            os.umask(0)
        else:
            os._exit(0)
    else:
        os._exit(0)
    maxfd = get_maxfd()
    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:    # ERROR, fd wasn't open to begin with (ignored)
            pass
    os.open(REDIRECT_TO, os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)


class Waker(object):

    def __init__(self):
        r, w = Pipe(duplex=False)
        _set_non_blocking(r.fileno())
        _set_non_blocking(w.fileno())
        close_on_exec(r.fileno())
        close_on_exec(w.fileno())
        self._writer = w
        self._reader = r

    def __str__(self):
        return 'Pipe waker %s' % self.fileno()

    def fileno(self):
        return self._reader.fileno()

    def wake(self):
        try:
            self._writer.send(b'x')
        except IOError:
            pass

    def consume(self):
        r = self._reader
        try:
            while r.poll():
                r.recv()
        except (IOError, EOFError):
            pass
