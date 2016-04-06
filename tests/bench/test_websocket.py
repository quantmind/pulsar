from random import randint
import unittest

from pulsar.utils.websocket import frame_parser


def i2b(args):
    return bytes(bytearray(args))


class TestCParser(unittest.TestCase):
    __benchmark__ = True
    __number__ = 10000
    _sizes = {'tiny': 10,
              'small': 200,
              'normal': 2000,
              'large': 10000,
              'huge': 100000}

    @classmethod
    def setUpClass(cls):
        size = cls.cfg.size
        nsize = cls._sizes[size]
        cls.data = i2b((randint(0, 255) for v in range(nsize)))

    def setUp(self):
        self.server = self.parser()
        self.client = self.parser(kind=1)

    def parser(self, kind=0):
        return frame_parser(kind=kind)

    def test_masked_encode(self):
        self.client.encode(self.data, opcode=2)


class TestPyParser(TestCParser):

    def parser(self, kind=0):
        return frame_parser(pyparser=True, kind=kind)
