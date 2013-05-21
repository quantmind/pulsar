'''Tests for the Backend Base class'''
from pulsar.apps.test import unittest
from pulsar.apps import Backend


class DummyBackend(Backend):
    
    @classmethod
    def path_from_scheme(cls, scheme):
        return 'tests.apps.backend'


class TestBackend(unittest.TestCase):
    
    def test_local(self):
        be = DummyBackend.make(name='foo')
        self.assertEqual(be.scheme, 'local')
        self.assertEqual(be.name, 'foo')
        self.assertEqual(be.connection_string, 'local://?name=foo')
        be2 = DummyBackend.make(name='foo')
        self.assertEqual(be.id, be2.id)