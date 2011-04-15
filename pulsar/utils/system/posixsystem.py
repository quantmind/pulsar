import fcntl
import resource
import grp
import pwd
import signal

from .base import *


import select
if hasattr(select,'epoll'):
    IOpoll = select.epoll
else:
    IOpoll = IOselect


SIGQUIT = signal.SIGQUIT

def chown(path, uid, gid):
    try:
        os.chown(path, uid, gid)
    except OverflowError:
        os.chown(path, uid, -ctypes.c_int(-gid).value)
        
        
def close_on_exec(fd):
    try:
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    except:
        pass
    
    
def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)
    
    
def get_maxfd():
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    return maxfd


def get_uid(user):
    if not user:
        return os.geteuid()
    elif user.isdigit() or isinstance(user, int):
        return int(user)
    else:
        return pwd.getpwnam(user).pw_uid
    
def get_gid(group):
    if not group:
        return os.getegid()
    elif group.isdigit() or isinstance(group, int):
        return int(group)
    else:
        return grp.getgrnam(group).gr_gid
    

def setpgrp():
    os.setpgrp()


def is_ipv6(addr):
    try:
        socket.inet_pton(socket.AF_INET6, addr)
    except socket.error: # not a valid address
        return False
    return True    


class UnixSocket(BaseSocket):
    
    FAMILY = socket.AF_UNIX
    
    def __init__(self, conf, fd=None):
        if fd is None:
            try:
                os.remove(conf.address)
            except OSError:
                pass
        super(UnixSocket, self).__init__(conf, fd=fd)
    
    def __str__(self):
        return "unix:%s" % self.address
        
    def bind(self, sock):
        old_umask = os.umask(self.conf.umask)
        sock.bind(self.address)
        system.chown(self.address, self.conf.uid, self.conf.gid)
        os.umask(old_umask)
        
    def close(self):
        super(UnixSocket, self).close()
        os.unlink(self.address)


def create_socket_address(addr):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. If it
    is a string, a Unix socket is created. Otherwise
    a TypeError is raised.
    """
    if isinstance(addr, tuple):
        if is_ipv6(addr[0]):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    elif isinstance(addr, basestring):
        sock_type = UnixSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)

    return sock_type



