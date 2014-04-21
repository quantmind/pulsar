"""A wait callback to allow psycopg2 cooperation with greenlet and asyncio

Use `make_asynchronous()` to enable greenlet support in Psycopg.
"""
from psycopg2 import ProgrammingError, OperationalError
from psycopg2 import extensions

from pulsar import ImproperlyConfigured

from . import wait_fd


def pulsar_wait_callback(conn):
    """A wait callback to allow greenlet to work with Psycopg.
    The caller must be from a greenlet other than the main one.
    """
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            # Done with waiting
            break
        elif state == extensions.POLL_READ:
            wait_fd(conn)
        elif state == extensions.POLL_WRITE:
            wait_fd(conn, read=False)
        else:
            raise OperationalError("Bad result from poll: %r" % state)


def make_asynchronous():
    try:
        extensions.POLL_OK
    except AttributeError:
        raise ImproperlyConfigured(
            'Psycopg2 does not have support for asynchronous connections. '
            'You need at least version 2.2.0 of Psycopg2.')
    extensions.set_wait_callback(pulsar_wait_callback)
