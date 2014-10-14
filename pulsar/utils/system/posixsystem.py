import os
import sys
import fcntl
import resource
import grp
import pwd
import signal
import socket
import ctypes
import errno
from multiprocessing import Pipe, current_process

from .base import *

__all__ = ['close_on_exec',
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
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]


def daemonize(auto_close_fds=True, keep_fds=None):    # pragma    nocover
    '''Standard daemonization of a process
    '''
    process_id = os.fork()
    if process_id < 0:
        # Fork error. Exit badly.
        sys.exit(1)
    elif process_id != 0:
        # This is the parent process. Exit.
        sys.exit(0)
    # This is the child process. Continue.

    # Stop listening for signals that the parent process receives.
    # This is done by getting a new process id.
    # setpgrp() is an alternative to setsid().
    # setsid puts the process in a new parent group and detaches
    # its controlling terminal.
    process_id = os.setsid()
    if process_id == -1:
        # Uh oh, there was a problem.
        sys.exit(1)
    #
    # Iterate through and close file descriptors
    if auto_close_fds:
        keep_fds = set(keep_fds or ())
        for fd in range(3, get_maxfd()):
            if fd not in keep_fds:
                try:
                    os.close(fd)
                except OSError:
                    pass

    devnull_fd = os.open(REDIRECT_TO, os.O_RDWR)
    for i in range(3):
        try:
            os.dup2(devnull_fd, i)
        except OSError as e:
            if e.errno != errno.EBADF:
                raise

    # Set umask to default to safe file permissions when running as a  daemon.
    # 027 is an octal number which we are typing as 0o27
    # for Python3 compatibility
    os.umask(0o27)
    os.close(devnull_fd)
