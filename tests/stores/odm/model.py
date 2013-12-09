from pulsar.apps.test import unittest
from pulsar.apps.data import odm



class TestModel(unittest.TestCase):

    def test_model(self):
        o = odm.Model(bla=1, foo=3)

