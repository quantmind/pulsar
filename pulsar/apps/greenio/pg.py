"""
Pulsar :mod:`~pulsar.apps.greenio` application can be used in conjunction
with :psycopg2coroutine:`psycopg2 coroutine support <>` to
query the :postgresql:`PostgreSql <>` database in an implicit
asynchronous fashion.
Such feature can be extremely useful since it allows to write
asynchronous code without the need to explicitly yield
control appropriately when necessary.

For example, one could use ORMs (Object Relational Mappers) such as
:sqlalchemy:`SqlAlchemy <>` and :django:`django <>` in asynchronous mode
by invoking :func:`make_asynchronous` somehere convinient in the code::

    from pulsar.apps.greenio import pg

    pg.make_asynchronous()

The :func:`make_asynchronous` set the :func:`psycopg2_wait_callback` function
as the waiting callback in psycopg2.

.. seealso::

    Check out how the :mod:`~pulsar.apps.pulse` application uses greenlet
    support to asynchrnously execute :django:`django <>` middleware when using
    :postgresql:`PostgreSql <>` as backend database.


.. autofunction:: psycopg2_wait_callback

.. autofunction:: make_asynchronous

"""
from psycopg2 import ProgrammingError, OperationalError
from psycopg2 import extensions

from pulsar import ImproperlyConfigured

from . import wait_fd


def psycopg2_wait_callback(conn):
    """A wait callback to allow greenlet to work with Psycopg.
    The caller must be from a greenlet other than the main one.
    """
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            # Done with waiting
            break
        elif state == extensions.POLL_READ:
            print('reading')
            wait_fd(conn)
        elif state == extensions.POLL_WRITE:
            print('writing')
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
    extensions.set_wait_callback(psycopg2_wait_callback)
