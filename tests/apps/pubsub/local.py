'''Tests the pubsub middleware and utilities.'''
import json

from pulsar.apps.pubsub import PubSub
from pulsar.apps.test import unittest, HttpTestClient


class DummyClient:
    
    def __init__(self):
        self.messages = []
        
    def write(self, message):
        self.messages.append(json.loads(message))

class pubsubTest(unittest.TestCase):
    
    @classmethod
    def backend(cls, id='test'):
        return 'local://?id=%s' % id
    
    def pubsub(self, id='test'):
        if not hasattr(self, '_pubsub'):
            self._pubsub = PubSub.make(self.backend(id))
        return self._pubsub
    
    def tearDown(self):
        return self.pubsub().close()
    
    def test_publish_no_clients(self):
        p = self.pubsub()
        clients = yield p.publish('messages foo', 'Hello world!')
        self.assertEqual(clients, 0)
        clients = yield p.publish('messages bla', 'Hello world!')
        self.assertEqual(clients, 0)
        
    def test_subscribe(self):
        p = self.pubsub()
        d = DummyClient()
        p.add_client(d)
        yield p.subscribe('messages')
        clients = yield p.publish('messages', 'Hello world!')
        self.assertEqual(clients, 1)
        self.assertEqual(d.messages[0]['message'], 'Hello world!')
        
    def test_pattern_subscribe(self):
        p = self.pubsub()
        d = DummyClient()
        p.add_client(d)
        yield p.subscribe('channel.*')
        clients = yield p.publish('channel.one', 'Hello world!')
        self.assertEqual(clients, 1)
        self.assertEqual(d.messages[0]['message'], 'Hello world!')
        yield p.subscribe('pippo', 'star.*')
        clients = yield p.publish('channel.one', 'Hello world again!')
        self.assertEqual(clients, 1)
        self.assertEqual(d.messages[1]['message'], 'Hello world again!')
        
    def test_unsubscribe(self):
        p = self.pubsub()
        p2 = PubSub.make(self.backend())
        yield p.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        yield p2.subscribe('hhhhhh')
        clients = yield p.publish('blaaa.ksjnkasjnkjs', 'ciao')
        self.assertEqual(clients, 1)
        clients = yield p.publish('*', 'bla')
        self.assertEqual(clients, 1)
        yield p.unsubscribe('blaaa.*')
        clients = yield p.publish('blaaa.ksjnkasjnkjs', 'ciao')
        self.assertEqual(clients, 0)
        clients = yield p.publish('fooo', 'ciao')
        self.assertEqual(clients, 1)
        result = yield p.close()
        self.assertEqual(len(result), 2)
        self.assertTrue('fooo' in result)
        self.assertTrue('hhhhhh' in result)
        clients = yield p.publish('*', 'bla')
        self.assertEqual(clients, 0)
        # close p2
        result = yield p2.close()
        self.assertEqual(len(result), 1)
        