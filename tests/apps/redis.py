'''Test twisted integration'''
from pulsar.utils.sockets import parse_connection_string
from pulsar.apps.redis import RedisClient, StrictRedis
from pulsar.apps.test import unittest

try:
    from stdnet import getdb
except ImportError:
    getdb = None
    

class RedisTestMixin(object):
    
    def _client(self, **options):
        schema, address, options = parse_connection_string(self.cfg.redis_server)
        self.assertEqual(schema, 'redis')
        return RedisClient(address, **options)
    
    def client(self, **options):
        raise NotImplementedError
    
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
        
        
@unittest.skipUnless(StrictRedis, 'Requires redis installed')
class RedisTest(RedisTestMixin, unittest.TestCase):
    
    def client(self, **options):
        return StrictRedis(connection_pool=self._client(**options))
            

@unittest.skipUnless(getdb, 'Requires stdnet installed')
class StednetTest(RedisTestMixin, unittest.TestCase):
    
    def getdb(self, **options):
        return getdb(self.cfg.redis_server, connection_pool=RedisClient)
    
    def client(self):
        return self.getdb().client
    