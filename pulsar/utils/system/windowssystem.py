import sys
import signal
import ctypes
import ctypes.wintypes
import errno
import socket
import getpass
from time import sleep

from .winprocess import WINEXE
from .base import *

__all__ = ['close_on_exec',
           'Waker',
           'daemonize',
           'socketpair',
           'EXIT_SIGNALS',
           'get_uid',
           'get_gid',
           'get_maxfd']

# See: http://msdn.microsoft.com/en-us/library/ms724935(VS.85).aspx
SetHandleInformation = ctypes.windll.kernel32.SetHandleInformation
SetHandleInformation.argtypes = (ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD,\
                                 ctypes.wintypes.DWORD)
SetHandleInformation.restype = ctypes.wintypes.BOOL

HANDLE_FLAG_INHERIT = 0x00000001

# The BREAK signal for windows
EXIT_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT, signal.SIGBREAK)
if sys.version_info >= (2, 7):
    SIG_NAMES[signal.CTRL_C_EVENT] = 'CTRL C EVENT'
    EXIT_SIGNALS += (signal.CTRL_C_EVENT,)
    
    
def get_parent_id():
    if ispy32:
        return os.getppid()
    else:
        return None

def chown(path, uid, gid):
    pass

def close_on_exec(fd):
    if fd:
        success = SetHandleInformation(fd, HANDLE_FLAG_INHERIT, 0)
        if not success:
            raise ctypes.GetLastError()
        
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

def socketpair(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0):
    """A socket pair usable as a self-pipe, for Windows.

    Origin: https://gist.github.com/4325783, by Geert Jansen.  Public domain.
    """
    # We create a connected TCP socket. Note the trick with setblocking(0)
    # that prevents us from having to create a thread.
    lsock = socket.socket(family, type, proto)
    lsock.bind(('localhost', 0))
    lsock.listen(1)
    addr, port = lsock.getsockname()
    csock = socket.socket(family, type, proto)
    csock.setblocking(True)
    try:
        csock.connect((addr, port))
    except Exception:
        lsock.close()
        csock.close()
        raise
    ssock, _ = lsock.accept()
    csock.setblocking(True)
    lsock.close()
    return (ssock, csock)


class Waker(object):
    '''In windows'''
    def __init__(self):
        self._reader, self._writer = socketpair()
        self._writer.setblocking(False)
        self._reader.setblocking(False)
        
    def __str__(self):
        return 'Socket waker {0}'.format(self.fileno())
        
    def fileno(self):
        return self._reader.fileno()
    
    def wake(self):
        try:
            self._writer.send(b'x')
        except IOError:
            pass
        
    def consume(self):
        try:
            result = True
            while result:
                result = self._reader.recv(1024)
        except IOError:
            pass

    def close(self):
        self.reader.close()
        self.writer.close()
        
