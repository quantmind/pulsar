import unittest

from . import Odm
from ..pulsards import StoreMixin


class TestPulsardsODM(StoreMixin, Odm, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        return cls.create_pulsar_store()
