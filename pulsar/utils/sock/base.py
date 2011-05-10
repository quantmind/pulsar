import logging
import socket
import time

__all__ = ['BaseSocket',
           'TCPSocket',
           'TCP6Socket']

logger = logging.getLogger('pulsar.sock')


MAXFD = 1024


class BaseSocket(object):
    
    def __init__(self, address, backlog = 2048, log = None, fd=None, bound=False):
        self.log = log or logger
        self.address = address
        self.backlog = backlog
        if fd is None:
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        else:
            if hasattr(socket,'fromfd'):
                sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
            else:
                raise ValueError('Cannot create socket from file deascriptor.\
 Not implemented in your system')
        self.sock = self.set_options(sock, bound=bound)
        
    def __getstate__(self):
        d = self.__dict__.copy()
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
    
    def __str__(self, name):
        return "<socket %d>" % self.sock.fileno()
    
    def __getattr__(self, name):
        return getattr(self.sock, name)
    
    def fileno(self):
        return self.sock.fileno()
    
    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not bound:
            self.bind(sock)
        sock.setblocking(0)
        sock.listen(self.backlog)
        return sock
        
    def bind(self, sock):
        sock.bind(self.address)
        
    def close(self):
        try:
            self.sock.close()
        except socket.error as e:
            self.log.info("Error while closing socket %s" % str(e))
        time.sleep(0.3)
        del self.sock


class TCPSocket(BaseSocket):
    
    FAMILY = socket.AF_INET
    
    def __str__(self):
        return "http://%s:%d" % self.sock.getsockname()
    
    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return super(TCPSocket, self).set_options(sock, bound=bound)


class TCP6Socket(TCPSocket):

    FAMILY = socket.AF_INET6

    def __str__(self):
        (host, port, fl, sc) = self.sock.getsockname()
        return "http://[%s]:%d" % (host, port)

