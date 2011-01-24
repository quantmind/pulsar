# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import ctypes
import os
import socket
import sys
import textwrap
import time
from time import sleep
from select import select as _select
try:
    from itertools import izip as zip
except ImportError:
    pass

from pulsar.utils.importer import import_module
from pulsar.utils.py2py3 import *

from .sock import *


ALL_SIGNALS = "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH"
MAXFD = 1024

if (hasattr(os, "devnull")):
   REDIRECT_TO = os.devnull
else:
   REDIRECT_TO = "/dev/null"

timeout_default = object()

CHUNK_SIZE = (16 * 1024)

MAX_BODY = 1024 * 132

weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
monthname = [None,
             'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Server and Date aren't technically hop-by-hop
# headers, but they are in the purview of the
# origin server which the WSGI spec says we should
# act like. So we drop them and add our own.
#
# In the future, concatenation server header values
# might be better, but nothing else does it and
# dropping them is easier.
hop_headers = set("""
    connection keep-alive proxy-authenticate proxy-authorization
    te trailers transfer-encoding upgrade
    server date
    """.split())
             
try:
    from setproctitle import setproctitle
    def set_proctitle(title):
        setproctitle("pulsar: %s" % title) 
except ImportError:
    def set_proctitle(title):
        return

def load_worker_class(uri):
    components = uri.split('.')
    if len(components) == 1:
        try:
            if uri.startswith("#"):
                uri = uri[1:]
            mod = import_module('pulsar.workers.' + uri)
            return getattr(mod,'Worker')
        except ImportError: 
            raise RuntimeError("arbiter uri invalid or not found")
    klass = components.pop(-1)
    mod = import_module('.'.join(components))
    return getattr(mod, klass)


def set_owner_process(uid,gid):
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
    
        
def parse_address(netloc, default_port=8000):
    netloc = to_string(netloc)
    if netloc.startswith("unix:"):
        return netloc.split("unix:")[1]

    # get host
    if '[' in netloc and ']' in netloc:
        host = netloc.split(']')[0][1:].lower()
    elif ':' in netloc:
        host = netloc.split(':')[0].lower()
    elif netloc == "":
        host = "0.0.0.0"
    else:
        host = netloc.lower()
    
    #get port
    netloc = netloc.split(']')[-1]
    if ":" in netloc:
        port = netloc.split(':', 1)[1]
        if not port.isdigit():
            raise RuntimeError("%r is not a valid port number." % port)
        port = int(port)
    else:
        port = default_port 
    return (host, port)

    
def daemonize():
    """\
    Standard daemonization of a process. Code is basd on the
    ActiveState recipe at:
        http://code.activestate.com/recipes/278731/
    """
    if not 'GUNICORN_FD' in os.environ:
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


class IObase(object):
    # Constants from the epoll module
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000
    _EPOLLONESHOT = (1 << 30)
    _EPOLLET = (1 << 31)

    # Our events map exactly to the epoll events
    NONE = 0
    READ = _EPOLLIN
    WRITE = _EPOLLOUT
    ERROR = _EPOLLERR | _EPOLLHUP | _EPOLLRDHUP
    
    def get_listener(self, arbiter):
        return None
    
    
class IOselect(IObase):
    
    def __init__(self):
        self.read_fds = set()
        self.write_fds = set()
        self.error_fds = set()
        self.fd_dict = {self.READ: self.read_fds,
                        self.WRITE: self.write_fds,
                        self.ERROR: self.error_fds}
    
    def register(self, fd, eventmask = None):
        eventmask = eventmask or self.READ
        self.fd_dict[eventmask].add(fd)
        
    def poll(self, timeout=None):
        triple = _select(self.read_fds,
                         self.write_fds,
                         self.error_fds,
                         timeout)
        events = {}
        for fds,etype in zip(triple,(self.READ,self.WRITE,self.ERROR)):
            for fd in fds:
                if fd not in events:
                    events[fd] = etype
        return iteritems(events)
    