from pulsar import test
from pulsar.http import rpc

from .calculator import run


class TestCalculatorExample(test.TestCase):
    
    def setUp(self):
        self.p = rpc.JsonProxy('http://localhost:8060')

