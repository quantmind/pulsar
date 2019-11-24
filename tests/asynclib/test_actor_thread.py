'''Tests actor and actor proxies.'''
import unittest

from tests.async.actor import ActorTest


class TestActorThread(ActorTest, unittest.TestCase):
    concurrency = 'thread'
