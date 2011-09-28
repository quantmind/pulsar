#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A utility class to write to and read from a non-blocking socket."""
import collections
import errno
import logging
import socket
import sys
import ssl

import pulsar

__all__ = ['AsyncIOStream']


iologger = logging.getLogger('pulsar.iostream')


class AsyncIOStream(pulsar.IOStream):
    """A utility class to write to and read from a non-blocking socket.

    We support three methods: write(), read_until(), and read_bytes().
    All of the methods take callbacks (since writing and reading are
    non-blocking and asynchronous). read_until() reads the socket until
    a given delimiter, and read_bytes() reads until a specified number
    of bytes have been read from the socket.

    The socket parameter may either be connected or unconnected.  For
    server operations the socket is the result of calling socket.accept().
    For client operations the socket is created with socket.socket(),
    and may either be connected before passing it to the IOStream or
    connected with IOStream.connect.

    A very simple (and broken) HTTP client using this class:

        from tornado import ioloop
        from tornado import iostream
        import socket

        def send_request():
            stream.write("GET / HTTP/1.0\r\nHost: friendfeed.com\r\n\r\n")
            stream.read_until("\r\n\r\n", on_headers)

        def on_headers(data):
            headers = {}
            for line in data.split("\r\n"):
               parts = line.split(":")
               if len(parts) == 2:
                   headers[parts[0].strip()] = parts[1].strip()
            stream.read_bytes(int(headers["Content-Length"]), on_body)

        def on_body(data):
            print data
            stream.close()
            ioloop.IOLoop.instance().stop()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = iostream.IOStream(s)
        stream.connect(("friendfeed.com", 80), send_request)
        ioloop.IOLoop.instance().start()

    """
    def blocking(self):
        return False
    
    def on_read(self):
        self._add_io_state(self.io_loop.READ)
        
    def on_write(self):
        self._add_io_state(self.ioloop.WRITE)
        
    def on_close(self):
        self.ioloop.remove_handler(self.socket.fileno())

    def connect(self, address, callback=None):
        """Connects the socket to a remote address without blocking.

        May only be called if the socket passed to the constructor was
        not previously connected.  The address parameter is in the
        same format as for socket.connect, i.e. a (host, port) tuple.
        If callback is specified, it will be called when the
        connection is completed.

        Note that it is safe to call IOStream.write while the
        connection is pending, in which case the data will be written
        as soon as the connection is ready.  Calling IOStream read
        methods before the socket is connected works on some platforms
        but is non-portable.
        """
        self._connecting = True
        try:
            self.socket.connect(address)
        except socket.error as e:
            # In non-blocking mode connect() always raises an exception
            if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise
        self._connect_callback = self.wrap(callback)
        self._add_io_state(self.ioloop.WRITE)

    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = stack_context.wrap(callback)

    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.socket:
            self.log.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.ioloop.READ:
                self.read()
            if not self.socket:
                return
            if events & self.ioloop.WRITE:
                if self._connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.socket:
                return
            if events & self.ioloop.ERROR:
                self.close()
                return
            state = self.ioloop.ERROR
            if self.reading():
                state |= self.ioloop.READ
            if self.writing():
                state |= self.ioloop.WRITE
            if state != self._state:
                self._state = state
                self.ioloop.update_handler(self.socket.fileno(), self._state)
        except:
            self.log.error("Uncaught exception, closing connection.",
                           exc_info=True)
            self.close()
            raise

    def read_from_buffer(self):
        """Attempts to complete the currently-pending read from the buffer.

        Returns True if the read was completed.
        """
        if self._read_bytes:
            if self._read_buffer_size() >= self._read_bytes:
                num_bytes = self._read_bytes
                callback = self._read_callback
                self._read_callback = None
                self._read_bytes = None
                self._run_callback(callback, self._consume(num_bytes))
                return True
        elif self._read_delimiter:
            _merge_prefix(self._read_buffer, sys.maxint)
            loc = self._read_buffer[0].find(self._read_delimiter)
            if loc != -1:
                callback = self._read_callback
                delimiter_len = len(self._read_delimiter)
                self._read_callback = None
                self._read_delimiter = None
                self._run_callback(callback,
                                   self._consume(loc + delimiter_len))
                return True
        return False

    def _handle_connect(self):
        if self._connect_callback is not None:
            callback = self._connect_callback
            self._connect_callback = None
            self._run_callback(callback)
        self._connecting = False

    def _add_io_state(self, state):
        if self.socket is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            with stack_context.NullContext():
                self.io_loop.add_handler(
                    self.socket.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.socket.fileno(), self._state)


class SSLIOStream(AsyncIOStream):
    """A utility class to write to and read from a non-blocking socket.

    If the socket passed to the constructor is already connected,
    it should be wrapped with
        ssl.wrap_socket(sock, do_handshake_on_connect=False, **kwargs)
    before constructing the SSLIOStream.  Unconnected sockets will be
    wrapped when IOStream.connect is finished.
    """
    def setup(self, kwargs):
        super(SSLIOStream,self).setup(kwargs)
        self._ssl_options = kwargs.pop('ssl_options', {})
        self._ssl_accepting = True
        self._handshake_reading = False
        self._handshake_writing = False

    def reading(self):
        return self._handshake_reading or super(SSLIOStream, self).reading()

    def writing(self):
        return self._handshake_writing or super(SSLIOStream, self).writing()

    def _do_ssl_handshake(self):
        # Based on code from test_ssl.py in the python stdlib
        try:
            self._handshake_reading = False
            self._handshake_writing = False
            self.socket.do_handshake()
        except ssl.SSLError as err:
            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                self._handshake_reading = True
                return
            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                self._handshake_writing = True
                return
            elif err.args[0] in (ssl.SSL_ERROR_EOF,
                                 ssl.SSL_ERROR_ZERO_RETURN):
                return self.close()
            elif err.args[0] == ssl.SSL_ERROR_SSL:
                logging.warning("SSL Error on %d: %s", self.socket.fileno(), err)
                return self.close()
            raise
        except socket.error as err:
            if err.args[0] == errno.ECONNABORTED:
                return self.close()
        else:
            self._ssl_accepting = False
            super(SSLIOStream, self)._handle_connect()

    def _handle_read(self):
        if self._ssl_accepting:
            self._do_ssl_handshake()
            return
        super(SSLIOStream, self)._handle_read()

    def _handle_write(self):
        if self._ssl_accepting:
            self._do_ssl_handshake()
            return
        super(SSLIOStream, self)._handle_write()

    def _handle_connect(self):
        self.socket = ssl.wrap_socket(self.socket,
                                      do_handshake_on_connect=False,
                                      **self._ssl_options)
        # Don't call the superclass's _handle_connect (which is responsible
        # for telling the application that the connection is complete)
        # until we've completed the SSL handshake (so certificates are
        # available, etc).


    def _read_from_socket(self):
        try:
            # SSLSocket objects have both a read() and recv() method,
            # while regular sockets only have recv().
            # The recv() method blocks (at least in python 2.6) if it is
            # called when there is nothing to read, so we have to use
            # read() instead.
            chunk = self.socket.read(self.read_chunk_size)
        except ssl.SSLError as e:
            # SSLError is a subclass of socket.error, so this except
            # block must come first.
            if e.args[0] == ssl.SSL_ERROR_WANT_READ:
                return None
            else:
                raise
        except socket.error as e:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return None
            else:
                raise
        if not chunk:
            self.close()
            return None
        return chunk




