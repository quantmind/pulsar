'''Classes for the pulsar Key-Value store server
'''
from functools import partial

import pulsar
from pulsar.apps.socket import SocketServer
from .parser import RedisParser


class PulsarStoreConnection(pulsar.Connection):
    '''Used both by client and server'''
    def __init__(self, parser_class, session, consumer_factory, producer,
                 timeout=0):
        super(PulsarStoreConnection, self).__init__(session, consumer_factory,
                                                    producer, timeout)
        self.parser = parser_class()


class PulsarStoreProtocol(pulsar.ProtocolConsumer):

    def data_received(self, data):
        parser = self._connection.parser
        parser.feed(data)
        response = parser.get()
        while response is not False:
            pass


class KeyValueStore(SocketServer):

    def connection_factory(self):
        return partial(PulsarStoreConnection, RedisParser)

    def protocol_consumer(self):
        '''Build the :class:`.ProtocolConsumer` factory.

        It uses the :class:`.HttpServerResponse` protocol consumer and
        the wsgi callable provided as parameter during initialisation.
        '''
        return partial(PulsarStoreProtocol, self.cfg)

    def monitor_start(self, monitor):
        cfg = self.cfg
        workers = min(1, cfg.workers)
        cfg.set('workers', workers)
        return super(KeyValueStore, self).monitor_start(monitor)
