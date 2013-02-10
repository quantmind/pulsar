import pulsar
from pulsar.apps.test import unittest
from pulsar.apps import httpqueue


class TestHttpQueue(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = httpqueue.server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                             backlog=1024, concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        cls.client = httpqueue.QueueClient('http://%s:%s' % cls.server.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
    
    def test_client(self):
        c = self.client
        self.assertTrue(str(c))
        
    def test_put(self):
        '''Test the creation of the queue server'''
        c = self.client
        response = c.put('Hello')
        yield response.on_finished
        self.assertEqual(response.status_code, 200)