'''pubsub redis backend.'''
from pulsar.apps.test import unittest
from pulsar.utils.sockets import parse_connection_string, get_connection_string

from . import local
from .local import DummyClient

try:
    import stdnet
except:
    stdnet = None
    

@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class pubsubTest(local.pubsubTest):
    
    @classmethod
    def backend(cls, tag):
        if tag:
            scheme, address, params = parse_connection_string(cls.cfg.redis_server)
            params['tag'] = tag
            return get_connection_string(scheme, address, params)
        else:
            return cls.cfg.redis_server
        
    def test_internal_subscribe(self):
        p = self.pubsub()
        self.assertFalse(p.backend.redis.consumer)
        result = yield p.subscribe('messages')
        self.assertTrue(p.backend.redis.consumer)
        result = yield p.subscribe('foo')
        clients = yield p.publish('messages', 'Hello world!')
        self.assertTrue(clients)