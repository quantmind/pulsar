'''Test twisted integration'''
import unittest

from .manage import twisted, mail_client


@unittest.skipUnless(twisted, 'Requires twisted and a config file')
class TestWebMail(unittest.TestCase):
    concurrency = 'thread'
    server = None

    def testMailCient(self):
        client = yield mail_client(self.cfg, timeout=5)
        self.assertTrue(client)
