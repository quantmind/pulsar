import unittest
import asyncio

from pulsar import Protocol


class TestPulsarProtocols(unittest.TestCase):

    def test_pulsar_protocol(self):
        proto = Protocol(asyncio.get_event_loop())
        self.assertEqual(proto.session, 1)
        self.assertFalse(proto.transport)
        self.assertTrue(proto.closed)
        transport = asyncio.Transport()
        proto.connection_made(transport)
        self.assertEqual(proto.transport, transport)
