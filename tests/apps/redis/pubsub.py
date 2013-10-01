'''Tests asynchronous PubSub.'''
import pulsar

from .client import RedisTest


class Listener(pulsar.Deferred):

    def __init__(self, channel, size):
        super(Listener, self).__init__()
        self.channel = channel
        self.size = size
        self.messages = []

    def on_message(self, channel_message):
        channel, message = channel_message
        channel = channel.decode('utf-8')
        if self.channel == channel:
            self.messages.append(message)
            if len(self.messages) == self.size:
                self.callback(self.messages)


class TestRedisPubSub(RedisTest):

    def test_subscribe_one(self):
        client = self.client()
        pubsub = client.pubsub()
        self.assertFalse(pubsub.connection)
        # Subscribe to one channel
        channels = yield pubsub.subscribe('blaaaaaa')
        self.assertTrue(pubsub.connection)
        self.assertEqual(channels, 1)
        channels = yield pubsub.subscribe('blaaaaaa')
        self.assertEqual(channels, 1)
        channels = yield pubsub.subscribe('foooo', 'jhkjhkjhkh')
        self.assertEqual(channels, 3)

    def test_subscribe_many(self):
        client = self.client()
        pubsub = client.pubsub()
        self.assertFalse(pubsub.connection)
        # Subscribe to one channel
        channels = yield pubsub.subscribe('blaaaaaa', 'foooo', 'hhhh', 'bla.*')
        self.assertTrue(pubsub.connection)
        self.assertEqual(channels, 4)
        channels = yield pubsub.unsubscribe('blaaaaaa', 'foooo')
        self.assertEqual(channels, 2)
        channels = yield pubsub.unsubscribe()
        self.assertEqual(channels, 0)

    def test_unsubscribe(self):
        client = self.client()
        pubsub = client.pubsub()
        channels = yield pubsub.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        self.assertEqual(channels, 3)
        channels = yield pubsub.subscribe('hhhhhh')
        self.assertEqual(channels, 3)
        #
        # Now unsubscribe
        channels = yield pubsub.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield pubsub.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield pubsub.unsubscribe()
        self.assertEqual(channels, 0)

    def test_publish(self):
        client = self.client()
        pubsub = client.pubsub()
        pubsub.subscribe('bla')
        result = yield pubsub.publish('bla', 'Hello')
        self.assertTrue(result>=0)

    def test_count_messages(self):
        client = self.client()
        pubsub = client.pubsub()
        result = yield pubsub.subscribe('counting')
        self.assertEqual(result, 1)
        listener = Listener('counting', 2)
        pubsub.bind_event('on_message', listener.on_message)
        result = yield pubsub.publish('counting', 'Hello')
        self.assertTrue(result>=0)
        pubsub.publish('counting', 'done')
        result = yield listener
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result), set((b'Hello', b'done')))

    def test_count_messages4(self):
        client = self.client()
        pubsub = client.pubsub()
        yield pubsub.subscribe('close')
        self.assertEqual(len(pubsub.channels), 1)
        listener = Listener('close', 4)
        pubsub.bind_event('on_message', listener.on_message)
        pubsub.publish('close', 'Hello')
        pubsub.publish('close', 'Hello2')
        pubsub.publish('close', 'Hello3')
        pubsub.publish('close', 'done')
        result = yield listener
        self.assertEqual(len(result), 4)
        self.assertEqual(set(result),
                         set((b'Hello', b'Hello2', b'Hello3', b'done')))

    def test_close(self):
        client = self.client()
        pubsub = client.pubsub()
        result = yield pubsub.subscribe('k1')
        self.assertEqual(result, 1)
        listener = Listener('k1', 2)
        pubsub.bind_event('on_message', listener.on_message)
        result = yield pubsub.publish('k1', 'Hello')
        self.assertTrue(result>=0)
        pubsub.publish('k1', 'done')
        result = yield listener
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result), set((b'Hello', b'done')))
        yield pubsub.close()
        self.assertEqual(pubsub._connection, None)
