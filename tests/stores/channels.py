import asyncio

from pulsar.apps.data.channels import Channels, StatusType, Json


class ChannelsTests:

    def channels(self):
        return Channels(self.store.pubsub(protocol=Json()),
                        namespace='testpulsar')

    async def test_channels(self):
        channels = self.channels()
        self.assertTrue(channels.pubsub)
        self.assertTrue(channels.status_channel)
        self.assertEqual(channels.status, StatusType.initialised)
        self.assertFalse(channels)
        self.assertEqual(list(channels), [])
        await channels.register('foo', '*', lambda c, e, d: d)
        self.assertTrue(channels)
        self.assertEqual(len(channels), 1)
        self.assertTrue('foo' in channels)
        self.assertEqual(channels.status, StatusType.initialised)

    async def test_wildcard(self):
        channels = self.channels()

        future = asyncio.Future()

        def fire(channel, event, data):
            future.set_result(event)

        await channels.register('test1', '*', fire)
        self.assertEqual(channels.status, StatusType.initialised)
        await channels.connect()
        self.assertEqual(channels.status, StatusType.connected)
        await channels.publish('test1', 'boom')
        result = await future
        self.assertEqual(result, 'boom')
        self.assertEqual(len(channels), 1)
        self.assertTrue(repr(channels))
