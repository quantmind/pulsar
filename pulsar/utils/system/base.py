import ctypes
import signal
from select import select as _select

from pulsar.utils.importer import import_module, module_attribute
from pulsar.utils.py2py3 import *

__all__ = ['ALL_SIGNALS',
           'SIG_NAMES',
           'set_proctitle',
           'set_owner_process',
           'parse_address',
           'IObase',
           'EpollProxy',
           'IOselect']


SIG_NAMES = {}
SKIP_SIGNALS = ('KILL','STOP')

def all_signals():
    for sig in dir(signal):
        if sig.startswith('SIG') and sig[3] != "_":
            val = getattr(signal,sig)
            if is_int(val):
                name = sig[3:]
                if name not in SKIP_SIGNALS:
                    SIG_NAMES[val] = name
                    yield name

            
ALL_SIGNALS = tuple(all_signals())


try:
    from setproctitle import setproctitle
    def set_proctitle(title):
        setproctitle(title)
        return True 
except ImportError:
    def set_proctitle(title):
        return


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
    '''Parse an address and return a tuple with host and port'''
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
        
    
class EpollProxy(object):
    '''An epoll like class. Idea from tornado.'''
    def __init__(self):
        self.read_fds = set()
        self.write_fds = set()
        self.error_fds = set()
        self.fd_dict = (self.read_fds, self.write_fds, self.error_fds)

    def register(self, fd, events):
        if events & IObase.READ:
            self.read_fds.add(fd)
        if events & IObase.WRITE:
            self.write_fds.add(fd)
        if events & IObase.ERROR:
            self.error_fds.add(fd)
            # Closed connections are reported as errors by epoll and kqueue,
            # but as zero-byte reads by select, so when errors are requested
            # we need to listen for both read and error.
            self.read_fds.add(fd)
                
    def modify(self, fd, events):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        self.read_fds.discard(fd)
        self.write_fds.discard(fd)
        self.error_fds.discard(fd)
    
    def poll(self, timeout=None):
        raise NotImplementedError
    
    
class IOselect(EpollProxy):
    '''An epoll like select class.'''
        
    def poll(self, timeout=None):
        readable, writeable, errors = _select(
            self.read_fds, self.write_fds, self.error_fds, timeout)
        return self.get_events(readable, writeable, errors)
    
    def get_events(self, readable, writeable, errors):
        events = {}
        for fd in readable:
            events[fd] = events.get(fd, 0) | IObase.READ
        for fd in writeable:
            events[fd] = events.get(fd, 0) | IObase.WRITE
        for fd in errors:
            events[fd] = events.get(fd, 0) | IObase.ERROR
        return list(iteritems(events))
    

        
    