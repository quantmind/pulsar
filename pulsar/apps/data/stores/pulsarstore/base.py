import logging
import socket
from functools import partial

from pulsar import (get_event_loop, ImproperlyConfigured, Pool, new_event_loop,
                    coroutine_return, get_application, in_loop, send,
                    EventHandler)
from pulsar.utils.importer import module_attribute
from pulsar.utils.pep import to_string
from pulsar.utils.httpurl import urlsplit, parse_qsl, urlunparse, urlencode

from ...server import KeyValueStore


data_stores = {}


class Compiler(object):
    '''Interface for :class:`Store` compilers.
    '''
    def __init__(self, store):
        self.store = store

    def compile_query(self, query):
        raise NotImplementedError

    def create_table(self, model_class):
        raise NotImplementedError


class Store(object):
    '''Base class for an asynchronous :ref:`data stores <data-stores>`.
    '''
    compiler_class = None
    default_manager = None

    def __init__(self, name, host, loop, database=None,
                 user=None, password=None, encoding=None, **kw):
        self._name = name
        self._host = host
        self._loop = loop
        self._encoding = encoding or 'utf-8'
        self._database = database
        self._user = user
        self._password = password
        self._urlparams = {}
        self._init(**kw)
        self._dns = self._buildurl()
        self.logger = loop.logger

    @property
    def name(self):
        '''Store name'''
        return self._name

    @property
    def database(self):
        '''Database name associated with this store.'''
        return self._database

    @property
    def encoding(self):
        '''Store encoding.'''
        return self._encoding

    @property
    def dns(self):
        '''Domain name server'''
        return self._dns

    @property
    def key(self):
        return (self._dns, self._encoding)

    def __repr__(self):
        return 'Store(dns="%s")' % self._dns
    __str__ = __repr__

    def router(self):
        '''Create a :class:`.Router` with this :class:`.Store` as
        default store.
        '''
        from asyncstore import odm
        return odm.Router(self)

    def connect(self):
        '''Connect with store server
        '''
        raise NotImplementedError

    def execute(self, *args, **options):
        '''Execute a command
        '''
        raise NotImplementedError

    def client(self):
        '''Get a client for the Store
        '''
        raise NotImplementedError

    def pubsub(self):
        '''Get a publish/subscribe handler for the Store
        '''
        raise NotImplementedError

    def create_database(self, dbname, **kw):
        '''Create a new database in this store.

        This is a pure virtual method and therefore only some :class:`Store`
        implementation expose it.
        '''
        raise NotImplementedError

    def create_table(self, model_class):
        '''Create the table for ``model_class``.
        '''
        pass

    def execute_transaction(self, commands):
        '''Execute a list of ``commands`` in a transaction
        '''
        raise NotImplementedError

    def compile_query(self, query):
        raise NotImplementedError

    def get_model(self, model, pk):
        '''Fetch an instance of a ``model`` with primary key ``pk``.
        '''
        raise NotImplementedError


    def _init(self, **kw):  # pragma    nocover
        pass

    def _buildurl(self):
        pre = ''
        if self._user:
            if not self._password:
                raise ImproperlyConfigured('user but not password')
            pre = '%s:%s@' % (self._user, self._password)
        elif self._password:
            raise ImproperlyConfigured('password but not user')
            assert self._password
        host = self._host
        if isinstance(host, tuple):
            host = '%s:%s' % host
        host = '%s%s' % (pre, host)
        path = '/%s' % self._database if self._database else ''
        query = urlencode(self._urlparams)
        return urlunparse((self._name, host, path, '', query, ''))

    def _build_pool(self):
        return Pool


class PubSubClient(object):
    '''Interface for a client of :class:`PubSub` handler.

    Instances of this :class:`Client` are callable object and are
    called once a new message has arrived from a subscribed channel.
    The callable accepts two parameters:

    * ``channel`` the channel which originated the message
    * ``message`` the message
    '''
    def __call__(self, channel, message):
        raise NotImplementedError


class PubSub(EventHandler):
    '''Asynchronous Publish/Subscriber handler interface.

    To listen for messages you can bind to the ``on_message`` event::

        pubsub = client.pubsub()
        pubsub.bind_event('on_message', handle_messages)
        pubsub.subscribe('mychannel')

    You can bind as many handlers to the ``on_message`` event as you like.
    The handlers receive one parameter only, a two-elements tuple
    containing the ``channel`` and the ``message``.
    '''
    MANY_TIMES_EVENTS = ('on_message',)

    def __init__(self, store, protocol=None):
        super(PubSub, self).__init__()
        self.store = store
        self._loop = store._loop
        self._protocol = protocol
        self._connection = None
        self._clients = set()
        self.bind_event('on_message', self._broadcast)

    def publish(self, channel, message):
        '''Publish a new ``message`` to a ``channel``.

        This method return a pulsar Deferred which results in the number of
        subscribers that will receive the message (the same behaviour as
        redis publish command).
        '''
        raise NotImplementedError

    def count(self, *channels):
        '''Returns the number of subscribers (not counting clients
        subscribed to patterns) for the specified channels.
        '''
        raise NotImplementedError

    def channels(self, pattern=None):
        '''Lists the currently active channels.

        An active channel is a Pub/Sub channel with one ore more subscribers
        (not including clients subscribed to patterns).
        If no ``pattern`` is specified, all the channels are listed,
        otherwise if ``pattern`` is specified only channels matching the
        specified glob-style pattern are listed.
        '''
        raise NotImplementedError

    def psubscribe(self, pattern, *patterns):
        '''Subscribe to a list of ``patterns``.
        '''
        raise NotImplementedError

    def punsubscribe(self, *channels):
        '''Unsubscribe from a list of ``patterns``.
        '''
        raise NotImplementedError

    def subscribe(self, channel, *channels):
        '''Subscribe to a list of ``channels``.
        '''
        raise NotImplementedError

    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels``.
        '''
        raise NotImplementedError

    def close(self):
        '''Stop listening for messages.
        '''
        raise NotImplementedError

    def add_client(self, client):
        '''Add a new ``client`` to the set of all :attr:`clients`.

        Clients must be callable. When a new message is received
        from the publisher, the :meth:`broadcast` method will notify all
        :attr:`clients` via the ``callable`` method.'''
        self._clients.add(client)

    def remove_client(self, client):
        '''Remove *client* from the set of all :attr:`clients`.'''
        self._clients.discard(client)

    ##    INTERNALS
    def _broadcast(self, response):
        '''Broadcast ``message`` to all :attr:`clients`.'''
        remove = set()
        channel = to_string(response[0])
        message = response[1]
        if self._protocol:
            message = self._protocol.dencode(message)
        for client in self._clients:
            try:
                client(channel, message)
            except IOError:
                remove.add(client)
            except Exception:
                self._loop.logger.exception(
                    'Exception while processing pub/sub client. Removing it.')
                remove.add(client)
        self._clients.difference_update(remove)


def parse_store_url(url):
    scheme, host, path, query, fr = urlsplit(url)
    assert not fr, 'store url must not have fragment, found %s' % fr
    assert scheme, 'Scheme not provided'
    # pulsar://
    if scheme == 'pulsar' and not host:
        host = '127.0.0.1:0'
    bits = host.split('@')
    assert len(bits) <= 2, 'Too many @ in %s' % url
    params = dict(parse_qsl(query))
    if path:
        database = path[1:]
        assert '/' not in database, 'Unsupported database %s' % database
        params['database'] = database
    if len(bits) == 2:
        userpass, host = bits
        userpass = userpass.split(':')
        assert len(userpass) == 2,\
            'User and password not in user:password format'
        params['user'] = userpass[0]
        params['password'] = userpass[1]
    else:
        user, password = None, None
    if ':' in host:
        host = tuple(host.split(':'))
    return scheme, host, params


def create_store(url, loop=None, force_sync=False, **kw):
    '''Create a new client :class:`Store` for a valid ``url``.

    A valid ``url`` taks the following forms::

        pulsar://user:password@127.0.0.1:6410
        redis://user:password@127.0.0.1:6500/11?namespace=testdb.
        postgresql://user:password@127.0.0.1:6500/testdb
        couchdb://user:password@127.0.0.1:6500/testdb

    :param loop: optional event loop, if not provided it is obtained
        via the ``get_event_loop`` method.
    :param force_sync: force a synchronous store.
    :param kw: additional key-valued parameters to pass to the :class:`Store`
        initialisation method.
    :return: a :class:`Store`.
    '''
    if isinstance(url, Store):
        return url
    scheme, address, params = parse_store_url(url)
    dotted_path = data_stores.get(scheme)
    if not dotted_path:
        raise ImproperlyConfigured('%s store not available' % scheme)
    if force_sync:
        logger = logging.getLogger(dotted_path)
        loop = new_event_loop(iothreadloop=False, logger=logger)
    else:
        loop = loop or get_event_loop()
    store_class = module_attribute(dotted_path)
    params.update(kw)
    return store_class(scheme, address, loop, **params)


def start_store(url, **kw):
    '''Equivalent to :func:`create_store` for most cases excepts when the
    ``url`` is for a pulsar store not yet started.

    In this case, the a :class:`.KeyValueStore` is started.
    '''
    store = create_store(url, **kw)
    if store.name == 'pulsar':
        client = store.client()
        try:
            yield client.ping()
        except socket.error:
            host = localhost(store._host)
            if not host:
                raise
            app = yield get_application('keyvaluestore')
            if not app:
                app = yield send('arbiter', 'run', KeyValueStore(bind=host))
            store._host = app.address
            dns = store._buildurl()
            store = create_store(dns, **kw)
    coroutine_return(store)


def localhost(host):
    if isinstance(host, tuple):
        if host[0] in ('127.0.0.1', ''):
            return ':'.join((str(b) for b in host))
    else:
        return host


def register_store(name, dotted_path):
    data_stores[name] = dotted_path
