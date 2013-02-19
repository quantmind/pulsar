'''Test twisted integration'''
from pulsar.utils.sockets import parse_connection_string
from pulsar.apps.redis import RedisClient, StrictRedis
from pulsar.apps.test import unittest

try:
    import stdnet
except ImportError:
    stdnet = None
    

class RedisTestMixin(object):
    
    def _client(self, **options):
        schema, address, options = parse_connection_string(self.cfg.redis_server)
        self.assertEqual(schema, 'redis')
        return RedisClient(address, **options)
    
    def client(self, **options):
        raise NotImplementedError
        
    
    
@unittest.skipUnless(StrictRedis, 'Requires redis installed')
class RedisTest(RedisTestMixin, unittest.TestCase):
    
    def client(self, **options):
        return StrictRedis(connection_pool=self._client(**options))
    
    def test_ping(self):
        client = self.client()
        pong = yield client.ping()
        self.assertEqual(pong, True)
        
    def test_get_set(self):
        client = self.client()
        result = yield client.set('a', 'foo')
        self.assertTrue(result)
        result = yield client.get('a')
        self.assertEqual(result, b'foo')
            

@unittest.skipUnless(stdnet, 'Requires stdnet installed')
class StednetTest(RedisTestMixin, unittest.TestCase):
    pass