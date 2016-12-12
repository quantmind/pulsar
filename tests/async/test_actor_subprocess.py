import unittest

from pulsar.apps.test import dont_run_with_thread, skipUnless
from pulsar.utils.system import platform

from tests.async.actor import ActorTest


@skipUnless(platform != 'win', 'Requires posix OS')
@dont_run_with_thread
class TestActorMultiProcess(ActorTest, unittest.TestCase):
    concurrency = 'subprocess'
