import unittest

from pulsar.apps.test import dont_run_with_thread, skipUnless
from pulsar.utils.system import platform

from tests.async.actor import ActorTest


@dont_run_with_thread
@skipUnless(platform.type != 'win', 'Requires posix OS')
class TestActorMultiProcess(ActorTest, unittest.TestCase):
    concurrency = 'subprocess'
