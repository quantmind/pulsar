import signal
import ctypes
import ctypes.wintypes
import errno
import socket
from time import sleep

from .winprocess import WINEXE
from .base import *

# See: http://msdn.microsoft.com/en-us/library/ms724935(VS.85).aspx
SetHandleInformation = ctypes.windll.kernel32.SetHandleInformation
SetHandleInformation.argtypes = (ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD,\
                                 ctypes.wintypes.DWORD)
SetHandleInformation.restype = ctypes.wintypes.BOOL

HANDLE_FLAG_INHERIT = 0x00000001

# The BREAK signal for windows
SIGQUIT = signal.SIGBREAK
   
    
def get_parent_id():
    if ispy32:
        return os.getppid()
    else:
        return None


def chown(path, uid, gid):
    pass


def close_on_exec(fd):
    success = SetHandleInformation(fd, HANDLE_FLAG_INHERIT, 0)
    if not success:
        raise ctypes.GetLastError()
    
    
def _set_non_blocking(fd):
    pass


def get_uid(user):
    return None


def get_gid(group):
    return None


def setpgrp():
    pass


class IOpoll(IOselect):
    
    def poll(self, timeout=None):
        """Win32 select wrapper."""
        if not (self.read_fds or self.write_fds):
            # windows select() exits immediately when no sockets
            if timeout is None:
                timeout = 0.01
            else:
                timeout = min(timeout, 0.001)
            sleep(timeout)
            return ()
        # windows doesn't process 'signals' inside select(), so we set a max
        # time or ctrl-c will never be recognized
        if timeout is None or timeout > 0.5:
            timeout = 0.5
        return super(IOpoll,self).poll(timeout)
    
    
class Waker(object):
    '''In windows '''
    def __init__(self):
        self._writer = socket.socket()
        # Disable buffering -- pulling the trigger sends 1 byte,
        # and we want that sent immediately, to wake up ASAP.
        self._writer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        count = 0
        while 1:
            count += 1
            s = socket.socket()
            s.bind(('127.0.0.1',0))
            addr = s.getsockname()
            s.listen(1)
            try:
                self._writer.connect(addr)
                break
            except socket.error as e:
                if e[0] != errno.WSAEADDRINUSE:
                    raise
                if count >= 10:  # I've never seen it go above 2
                    s.close()
                    self.writer.close()
                    raise socket.error("Cannot bind trigger!")
                s.close()
        
        self._reader, addr = s.accept()
        self._writer.setblocking(False)
        self._reader.setblocking(False)
        s.close()
        
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
    
    
def daemonize():
    pass
