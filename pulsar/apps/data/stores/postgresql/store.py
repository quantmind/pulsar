'''Asynchronous Postgresql psycopg2 dialect'''
import os
import sys
from functools import partial
from collections import deque

import psycopg2
from psycopg2 import ProgrammingError, OperationalError
from psycopg2.extensions import (connection as base_connection,
                                 cursor as base_cursor,
                                 POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR,
                                 TRANSACTION_STATUS_IDLE)

import pulsar
from pulsar import coroutine_return, Deferred, Pool, Failure
from pulsar.apps.data import register_store

from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

from ...odm import sql

try:
    psycopg2.extensions.POLL_OK
except AttributeError:
    raise ImproperlyConfigured(
        'Psycopg2 does not have support for asynchronous connections. '
        'You need at least version 2.2.0 of Psycopg2.')


class Async(object):

    def wait(self, callback, errback=None, registered=False):
        exc = None
        loop = self._loop
        try:
            state = self.connection.poll()
        except Exception:
            exc = sys.exc_info()
            if registered:
                loop.remove_connector(self._sock_fd)
        else:
            if state == POLL_OK:
                if registered:
                    loop.remove_connector(self._sock_fd)
                callback()
            elif not registered:
                loop.add_connector(self._sock_fd, self.wait, callback,
                                   errback, True)
            return
        self.close()
        exc = Failure(exc)
        if errback:
            errback(exc)


class Cursor(base_cursor, Async):
    '''Asynchronous Cursor
    '''
    def __init__(self, loop, *args, **kw):
        super(Cursor, self).__init__(*args, **kw)
        self._sock_fd = self.connection.fileno()
        self._loop = loop
        self.waiting = None

    def begin(self):
        return self.execute('BEGIN')

    def commit(self):
        return self.execute('COMMIT')

    def rollback(self):
        return self.execute('ROLLBACK')

    def execute(self, *args, **kw):
        self.waiting = waiting = Deferred(self._loop)
        super(Cursor, self).execute(*args, **kw)
        self.wait(self._result, self._error)
        return waiting

    def _result(self):
        self.waiting.callback(None)

    def _error(self, exc):
        self.waiting.callback(exc)


class Connection(sql.SqlConnection, Async):
    '''A wrapper for a psycopg2 connection'''
    def __getattr__(self, name):
        return getattr(self.connection, name)


class PostgreSql(sql.SqlDB):
    dbapi = psycopg2

    def _init(self, pool_size=50, namespace=None, **kwargs):
        self._received = 0
        self.dialect = PGDialect_psycopg2()
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self.connect, loop=self._loop, pool_size=pool_size)

    def connect(self):
        '''Create a new connection'''
        conn = Connection(self, self.raw_connection())
        future = Deferred()
        conn.wait(lambda: future.callback(conn), future.callback)
        return future

    def raw_connection(self):
        cursor_factory = partial(Cursor, self._loop)
        host, port = self._host
        database=  self._database or 'postgres'
        return self.dbapi.connect(host=host,
                                  port=port,
                                  user=self._user,
                                  password=self._password,
                                  database=self._database or 'postgres',
                                  async=1,
                                  cursor_factory=cursor_factory)



register_store('postgresql',
               'pulsar.apps.data.stores.postgresql.store.PostgreSql')
