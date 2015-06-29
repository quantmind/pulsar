'''\
Pulsar key-value store server. To run the server type::

    python manage.py

Open a new shell and launch python and type::

    >>> from pulsar.apps.data import create_store
    >>> store = create_store('pulsar://localhost:6410')
    >>> client = store.client()
    >>> client.ping()
    True
    >>> client.echo('Hello!')
    b'Hello!'
    >>> client.set('bla', 'foo')
    True
    >>> client.get('bla')
    b'foo'
    >>> client.dbsize()
    1

'''
from pulsar.apps.ds import PulsarDS


if __name__ == '__main__':  # pragma nocover
    PulsarDS().start()
