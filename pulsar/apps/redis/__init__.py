'''
Asynchronous Redis client, requires redis-py_.

Usage
=============

The main class for the asynchronous redis client is :class:`RedisPool`
which is a pool of different redis clients. An instance of this class can be
created as a singleton somewhere in your code::

    from pulsar.apps.redis import RedisPool

    pool = RedisPool()

To create a new redis client::

    >>> client = pool.redis(('localhost', 6379), db=7)
    >>> d = client.echo('Hello')

API
======

RedisPool
~~~~~~~~~~~~~~~~~~

.. autoclass:: RedisPool
   :members:
   :member-order: bysource

Clients
==============

.. automodule:: pulsar.apps.redis.client

.. _redis-py: https://github.com/andymccurdy/redis-py
'''
from collections import namedtuple
from functools import partial
from itertools import chain

import pulsar
from pulsar.utils.internet import parse_connection_string

try:
    from .client import Redis, RedisProtocol, CRedisParser, Request
except ImportError:
    RedisProtocol = None
    RedisParser = None
    Redis = None
    Request = None


connection_info = namedtuple('connection_info', 'address db password timeout')


class RedisPool(pulsar.Client):
    '''A :class:`pulsar.Client` to manage clients for several redis servers.

    :param encoding: default charset encoding for this pool of clients. If
        not provided ``utf-8`` is used.
    :param parser: optional parser factory for this redis pool.

    A :class:`RedisPool`
    '''
    consumer_factory = RedisProtocol

    def __init__(self, encoding=None, parser=None, encoding_errors='strict',
                 **kwargs):
        super(RedisPool, self).__init__(**kwargs)
        self.parser = parser or CRedisParser
        self.encoding = encoding or 'utf-8'
        self.encoding_errors = encoding_errors or 'strict'
        self.bind_event('pre_request', self._authenticate)

    def redis(self, address, db=0, password=None, timeout=None, **kw):
        '''Return a :class:`Redis` client.

        :param address: the address of the server.
        :param db: optional server database number.
        :param password: optional server password.
        :param timeout: optional timeout for idle connections.
        :return: a redis-py_ client.
        '''
        assert Redis, 'To use pulsar-redis you need redis-py installed'
        timeout = int(timeout or self.timeout)
        info = connection_info(address, db, password, timeout)
        return Redis(self, info, **kw)

    def from_connection_string(self, connection_string, **kw):
        scheme, address, params = parse_connection_string(connection_string)
        if scheme == 'redis':
            params.update(kw)
            return self.redis(address, **params)
        else:
            raise ValueError('Use "redis" as connection string schema')

    def request(self, client, command_name, args, options=None, response=None,
                new_connection=False, **inp_params):
        request = Request(client, command_name, args, options, **inp_params)
        resp = self.response(request, response, new_connection)
        if resp is not response and not client.full_response:
            on_finished = resp.on_finished
            on_finished.add_callback(lambda r: r.result)
            return on_finished
        else:
            return resp

    def request_pipeline(self, pipeline, raise_on_error=True):
        commands = pipeline.command_stack
        if not commands:
            return ()
        if pipeline.transaction:
            commands = list(chain([(('MULTI', ), {})], commands,
                                  [(('EXEC', ), {})]))
        request = Request(pipeline, '', commands,
                          raise_on_error=raise_on_error)
        response = self.response(request)
        if not pipeline.full_response:
            on_finished = response.on_finished
            on_finished.add_callback(lambda r: r.result)
            return on_finished
        else:
            return response

    def _next(self, consumer, next_request, result):
        consumer.new_request(next_request)

    #    INTERNALS
    def _authenticate(self, response):
        # Perform redis authentication as a pre_request event
        if response._connection.processed <= 1:
            request = response._request
            client = request.client
            if request.is_pipeline:
                client = client.client
            connection_info = client.connection_info
            if connection_info.password:
                return self.request(
                    client, 'auth', (connection_info.password,), {},
                    release_connection=False,
                    post_request=partial(self._change_db, request),
                    response=response)
            elif connection_info.db:
                return self._change_db(None, response)
        return response

    def _change_db(self, request, response):
        if not request:
            request = response._request
        else:
            response = self._chain_response(response)
        client = request.client
        connection_info = client.connection_info
        if connection_info.db:
            return self.request(
                client, 'select', (connection_info.db,), {},
                release_connection=False,
                post_request=partial(self._continue, request),
                response=response)
        else:
            return self._continue(request, response)

    def _continue(self, request, response):
        response = self._chain_response(response)
        return request.connection_pool.response(request, response, False)

    def _chain_response(self, prev_response):
        connection = prev_response.connection
        response = self.build_consumer()
        connection.set_consumer(None)
        connection.set_consumer(response)
        response.chain_event(prev_response, 'post_request')
        return response
