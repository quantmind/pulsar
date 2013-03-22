'''Test twisted integration'''
import pulsar
from pulsar import is_failure, is_async
from pulsar.utils.pep import to_bytes, to_string
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import twisted, config
        
    
@unittest.skipUnless(twisted and config, 'Requires twisted and a config file')
class TestWebMail(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    def testMailCient(self):
        client = mail_client(timeout=5)
        yield client
        client = client.result
        self.assertTrue(client)