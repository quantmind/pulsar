'''pubsub local backend.'''
import time

from pulsar import Deferred
from pulsar.apps.pubsub import PubSub
from pulsar.apps.test import unittest, HttpTestClient
from pulsar.utils.security import gen_unique_id
from pulsar.utils.system import json


class DummyClient1(Deferred):

    def __call__(self, channels, message):
        self.callback(message)


class DummyClient2(Deferred):

    def __call__(self, channels, message):
        self.callback(json.loads(message))


def json_encoder(message):
    v =  {'message': message,
          'time': time.time()}
    return json.dumps(v)


class pubsubTest(unittest.TestCase):

    @classmethod
    def backend(cls, tag):
        return 'local://?tag=%s' % tag

    def pubsub(self, tag=None, **kwargs):
        tag = tag or gen_unique_id()
        ps = PubSub(self.backend(tag), **kwargs)
        self._pubsub.append(ps)
        return ps

    def setUp(self):
        self._pubsub = []

    def tearDown(self):
        for p in self._pubsub:
            p.close()

    def test_subscribe(self):
        p = self.pubsub()
        d = DummyClient1()
        p.add_client(d)
        self.assertTrue(len(p.clients), 1)
        yield p.subscribe('messages')
        clients = yield p.publish('messages', 'Hello world!')
        self.assertTrue(clients)
        message = yield d
        self.assertEqual(message, 'Hello world!')
        p.remove_client(d)
        self.assertFalse(p.clients)

    def test_subscribe_encoder(self):
        p = self.pubsub(encoder=json_encoder)
        d = DummyClient2()
        p.add_client(d)
        self.assertTrue(len(p.clients), 1)
        yield p.subscribe('json')
        clients = yield p.publish('json', 'Hello world!')
        self.assertTrue(clients)
        message = yield d
        self.assertEqual(message['message'], 'Hello world!')
        p.remove_client(d)
        self.assertFalse(p.clients)

    def test_same_id(self):
        p1 = self.pubsub('bla')
        p2 = self.pubsub('bla')
        self.assertEqual(p1.id, p2.id)

    def test_backend(self):
        p = self.pubsub()
        backend = p.backend
        self.assertEqual(backend.name, 'arbiter')
        self.assertTrue('tag=' in backend.connection_string)
        self.assertTrue(backend.params['tag'])

    def test_same_backends(self):
        p = self.pubsub('test')
        p2 = self.pubsub('test')
        self.assertNotEqual(p, p2)
        self.assertEqual(p.backend, p2.backend)
        self.assertEqual(id(p.clients), id(p2.clients))

    def test_different_backends(self):
        p = self.pubsub('test1')
        p2 = self.pubsub('test2')
        self.assertNotEqual(p, p2)
        self.assertNotEqual(p.backend, p2.backend)
        self.assertNotEqual(id(p.clients), id(p2.clients))

    def test_publish_no_clients(self):
        p = self.pubsub()
        clients = yield p.publish('messages foo', 'Hello world!')
        self.assertEqual(clients, 0)
        clients = yield p.publish('messages bla', 'Hello world!')
        self.assertEqual(clients, 0)

    def test_pattern_subscribe(self):
        p = self.pubsub()
        d = DummyClient1()
        p.add_client(d)
        result = yield p.subscribe('channel.*')
        clients = yield p.publish('channel.one', 'Hello world!')
        message = yield d
        self.assertEqual(message, 'Hello world!')
        result = yield p.subscribe('pippo', 'star.*')
        p.remove_client(d)
        d = DummyClient1()
        p.add_client(d)
        clients = yield p.publish('channel.one', 'Hello world again!')
        message = yield d
        self.assertEqual(message, 'Hello world again!')

    def test_unsubscribe_all(self):
        p = self.pubsub()
        channels = yield p.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        self.assertEqual(channels, 3)
        channels = yield p.unsubscribe()
        self.assertEqual(channels, 0)

    def test_unsubscribe_0(self):
        p = self.pubsub()
        channels = yield p.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        self.assertEqual(channels, 3)
        channels = yield p.unsubscribe('bo')
        self.assertEqual(channels, 3)
        channels = yield p.unsubscribe('foo.*')
        self.assertEqual(channels, 3)

    def test_unsubscribe(self):
        p = self.pubsub()
        p2 = self.pubsub()
        channels = yield p.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        self.assertEqual(channels, 3)
        channels = yield p2.subscribe('hhhhhh')
        self.assertEqual(channels, 1)
        #
        # Now unsubscribe
        channels = yield p.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield p.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield p.unsubscribe()
        self.assertEqual(channels, 0)
        channels = yield p2.unsubscribe('blaaanskjnk')
        self.assertEqual(channels, 1)
        channels = yield p2.unsubscribe('hhhhhh')
        self.assertEqual(channels, 0)
