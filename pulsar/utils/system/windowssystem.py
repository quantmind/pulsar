import signal
from time import sleep
#import multiprocessing.reduction

from .base import *


SIGQUIT = signal.SIGTERM
   
    
def get_parent_id():
    if ispy32:
        return os.getppid()
    else:
        return None


def chown(path, uid, gid):
    pass


def close_on_exec(fd):
    pass
    
    
def __set_non_blocking(fd):
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
    
    
def daemonize():
    pass
