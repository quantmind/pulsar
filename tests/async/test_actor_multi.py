import unittest

from pulsar.apps.test import dont_run_with_thread

from tests.async.actor import ActorTest


@dont_run_with_thread
class TestActorMultiProcess(ActorTest, unittest.TestCase):
    concurrency = 'multi'
