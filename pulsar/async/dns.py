import socket
from functools import partial

from .futures import get_event_loop, Future

try:
    import pycares
except ImportError:
    pycares = None


def resolver(loop, cfg=None):
    return StandardDNSResolver(loop)
    no_pycares = cfg.no_pycares if cfg else False
    if pycares is None or no_pycares:
        return StandardDNSResolver(loop)
    else:
        return DNSResolver(loop)


class StandardDNSResolver(object):

    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()

    def getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        return self._loop.run_in_executor(None, socket.getaddrinfo, host,
                                          port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self._loop.run_in_executor(None, socket.getnameinfo,
                                          sockaddr, flags)


class DNSResolver(StandardDNSResolver):
    '''A dns resolver which uses pycares
    https://github.com/saghul/pycares
    '''
    EVENT_READ = 0
    EVENT_WRITE = 1

    def __init__(self, loop=None):
        self._channel = pycares.Channel(sock_state_cb=self._sock_state_cb)
        self._timer = None
        self._fds = set()
        self._loop = loop or get_event_loop()

    def getnameinfo(self, sockaddr, flags=0):
        future = Future(loop=self._loop)
        self._channel.getnameinfo(sockaddr, flags,
                                  partial(self._getnameinfo, future, sockaddr))
        return future

    def _sock_state_cb(self, fd, readable, writable):
        if readable or writable:
            if readable:
                self._loop.add_reader(fd, self._process_events, fd,
                                      self.EVENT_READ)
            if writable:
                self._loop.add_writer(fd, self._process_events, fd,
                                      self.EVENT_WRITE)
            self._fds.add(fd)
            if self._timer is None:
                self._timer = self._loop.call_later(1.0, self._timer_cb)
        else:
            # socket is now closed
            self._fds.discard(fd)
            if not self._fds:
                self._timer.cancel()
                self._timer = None

    def _timer_cb(self):
        self._channel.process_fd(pycares.ARES_SOCKET_BAD,
                                 pycares.ARES_SOCKET_BAD)
        self._timer = self._loop.call_later(1.0, self._timer_cb)

    def _process_events(self, fd, event):
        if event == self.EVENT_READ:
            read_fd = fd
            write_fd = pycares.ARES_SOCKET_BAD
        elif event == self.EVENT_WRITE:
            read_fd = pycares.ARES_SOCKET_BAD
            write_fd = fd
        else:
            read_fd = write_fd = pycares.ARES_SOCKET_BAD
        self._channel.process_fd(read_fd, write_fd)

    def _getnameinfo(self, future, sockaddr, result, error):
        if error:
            future.set_exception(
                Exception('C-Ares returned error %s: %s while resolving %s' %
                          (error, pycares.errno.strerror(error),
                           str(sockaddr))))
        else:
            service = result.service
            if not service:
                service = 'https' if sockaddr[1] == 443 else 'http'
            future.set_result((result.node, service))
