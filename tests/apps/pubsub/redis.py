'''pubsub redis backend.'''
from pulsar.apps.test import unittest
from pulsar.apps.redis import RedisPool
from pulsar.utils.internet import parse_connection_string, get_connection_string

from . import local

@unittest.skipUnless(RedisPool.consumer_factory,
                     'Requires redis-py installed')
class pubsubTest(local.pubsubTest):

    @classmethod
    def backend(cls, namespace):
        if namespace:
            backend = cls.cfg.backend_server or 'redis://127.0.0.1:6379'
            scheme, address, params = parse_connection_string(backend)
            params['namespace'] = namespace
            return get_connection_string(scheme, address, params)
        else:
            return cls.cfg.backend_server

    def test_backend(self):
        p = self.pubsub()
        backend = p.backend
        self.assertEqual(backend.name, 'arbiter')
        self.assertTrue('namespace=' in backend.connection_string)
        self.assertTrue(backend.params['namespace'])
