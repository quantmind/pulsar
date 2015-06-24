import os
import sys
import fcntl
import resource
import grp
import pwd
import signal
import socket
import ctypes
from multiprocessing import current_process

from .base import *     # noqa

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


def daemonize(enable_stdio_inheritance=False,
              auto_close_fds=True,
              keep_fds=None):   # pragma    nocover
    """\
    Standard daemonization of a process.
    http://www.svbug.com/documentation/comp.unix.programmer-FAQ/faq_2.html#SEC16
    """
    if os.fork():
        os._exit(0)
    os.setsid()

    if os.fork():
        os._exit(0)

    os.umask(0o22)

    # In both the following any file descriptors above stdin
    # stdout and stderr are left untouched. The inheritence
    # option simply allows one to have output go to a file
    # specified by way of shell redirection when not wanting
    # to use --error-log option.

    if not enable_stdio_inheritance:
        # Remap all of stdin, stdout and stderr on to
        # /dev/null. The expectation is that users have
        # specified the --error-log option.

        if keep_fds:
            keep_fds = set(keep_fds)
            for fd in range(0, 3):
                if fd not in keep_fds:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
        else:
            os.closerange(0, 3)

        fd_null = os.open(REDIRECT_TO, os.O_RDWR)

        if fd_null != 0:
            os.dup2(fd_null, 0)

        os.dup2(fd_null, 1)
        os.dup2(fd_null, 2)

    else:
        fd_null = os.open(REDIRECT_TO, os.O_RDWR)

        # Always redirect stdin to /dev/null as we would
        # never expect to need to read interactive input.

        if fd_null != 0:
            os.close(0)
            os.dup2(fd_null, 0)

        # If stdout and stderr are still connected to
        # their original file descriptors we check to see
        # if they are associated with terminal devices.
        # When they are we map them to /dev/null so that
        # are still detached from any controlling terminal
        # properly. If not we preserve them as they are.
        #
        # If stdin and stdout were not hooked up to the
        # original file descriptors, then all bets are
        # off and all we can really do is leave them as
        # they were.
        #
        # This will allow 'gunicorn ... > output.log 2>&1'
        # to work with stdout/stderr going to the file
        # as expected.
        #
        # Note that if using --error-log option, the log
        # file specified through shell redirection will
        # only be used up until the log file specified
        # by the option takes over. As it replaces stdout
        # and stderr at the file descriptor level, then
        # anything using stdout or stderr, including having
        # cached a reference to them, will still work.

        def redirect(stream, fd_expect):
            try:
                fd = stream.fileno()
                if fd == fd_expect and stream.isatty():
                    os.close(fd)
                    os.dup2(fd_null, fd)
            except AttributeError:
                pass

        redirect(sys.stdout, 1)
        redirect(sys.stderr, 2)
