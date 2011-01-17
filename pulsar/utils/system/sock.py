# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import errno
import logging
import os
import socket
import sys
import time

log = logging.getLogger(__name__)

__all__ = ['BaseSocket',
           'TCPSocket',
           'TCP6Socket']

class BaseSocket(object):
    
    def __init__(self, conf, fd=None):
        self.conf = conf
        self.address = conf.address
        if fd is None:
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        else:
            sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
        self.sock = self.set_options(sock, bound=(fd is not None))
    
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
        sock.listen(self.conf.backlog)
        return sock
        
    def bind(self, sock):
        sock.bind(self.address)
        
    def close(self):
        try:
            self.sock.close()
        except socket.error as e:
            log.info("Error while closing socket %s" % str(e))
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

