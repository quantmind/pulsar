'''Tests actor and actor proxies.'''
import unittest

from pulsar.apps.test import dont_run_with_thread

from tests.async.actor import ActorTest


class TestActorThread(ActorTest, unittest.TestCase):
    concurrency = 'thread'


@dont_run_with_thread
class TestActorProcess(TestActorThread):
    concurrency = 'process'
