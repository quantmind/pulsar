'''Asynchronous Postgresql psycopg2 dialect'''
import os
import sys
from functools import partial

import psycopg2
from psycopg2 import ProgrammingError, OperationalError
from psycopg2.extensions import (connection as base_connection,
                                 cursor as base_cursor,
                                 POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR,
                                 TRANSACTION_STATUS_IDLE)

import pulsar
from pulsar import coroutine_return, Deferred
from pulsar.apps.data import register_store, sql

try:
    psycopg2.extensions.POLL_OK
except AttributeError:
    raise ImproperlyConfigured(
        'Psycopg2 does not have support for asynchronous connections. '
        'You need at least version 2.2.0 of Psycopg2.')


class Async(AsyncBase):

    def wait(self, callback, errback=None, registered=False):
        exc = None
        try:
            state = self.connection.poll()
        except Exception:
            exc = sys.exc_info()
        else:
            loop = self._loop
            if state == POLL_OK:
                if registered:
                    loop.remove_connector(self._sock_fd)
                callback()
            elif not registered:
                loop.add_connector(self._sock_fd, self.wait, callback,
                                   errback, True)
            return
        self.close(exc)
        if errback:
            errback(exc)


class Cursor(base_cursor, Async):
    '''Asynchronous Cursor
    '''
    def __init__(self, loop, *args, **kw):
        self._loop = loop
        super(Cursor, self).__init__(*args, **kw)

    def begin(self):
        return self.execute('BEGIN')

    def commit(self):
        return self.execute('COMMIT')

    def rollback(self):
        return self.execute('ROLLBACK')

    def execute(self, *args, **kw):
        super(Cursor, self).execute(*args, **kw)
        future = Deferred()
        conn.wait(lambda: future.callback, future.callback)
        return future


class Connection(Async):
    '''A wrapper for a psycopg2 connection'''
    def __init__(self, loop, conn):
        self._loop = _loop
        self._connection = conn
        self._closing = False
        self._sock_fd = conn.fileno()

    @property
    def info(self):
        return self._connection.info

    def close(self, exc=None):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        :class:`BaseProtocol.connection_lost` method will (eventually) called
        with ``None`` as its argument.
        """
        if not self._closing:
            self._closing = True
            try:
                self.connection.close()
            except Exception:
                pass
            self._event_loop.remove_connector(self._sock_fd)
            self.connection = None
            self._sock_fd = None

    def cursor(self, **kwargs):
        return self.connection.cursor(**kwargs)

    def __getattr__(self, name):
        return getattr(self.connection, name)


class PostgreSql(sql.SqlDB):
    dbapi = psycopg2

    def _init(self, pool_size=50, **kwargs):
        self._received = 0
        self.dbapi = psycopg2
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self.connect, pool_size=pool_size)

    def connect(self):
        '''Create a new Redis connection'''
        kw['async'] = 1
        kw['cursor_factory'] = partial(Cursor, self._event_loop)
        conn = Connection(self._event_loop, self.dbapi.connect(*args, **kw))
        future = Deferred()
        conn.wait(lambda: future.callback(conn), future.callback)
        return future


register_store('pulsar',
               'pulsar.apps.data.stores.postgresql.PostgreSql')
