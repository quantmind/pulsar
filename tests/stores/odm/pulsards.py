import unittest

from . import Odm
from ..testmodels import User
from ..pulsards import StoreMixin


class TestPulsardsODM(StoreMixin, Odm, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        return cls.create_pulsar_store()
