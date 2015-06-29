from random import choice
import string
import unittest

from pulsar import HAS_C_EXTENSIONS
from pulsar.apps.ds import redis_parser

characters = string.ascii_letters + string.digits


class RedisPyParser(unittest.TestCase):
    __benchmark__ = True
    __number__ = 100
    _sizes = {'tiny': 2,
              'small': 10,
              'normal': 100,
              'big': 1000,
              'huge': 10000}
    redis_py_parser = True

    @classmethod
    def setUpClass(cls):
        size = cls.cfg.size
        nsize = cls._sizes[size]
        cls.data = [''.join((choice(characters) for l in range(20)))
                    for s in range(nsize)]
        cls.data_bytes = [(''.join((choice(characters) for l in range(20)))
                           ).encode('utf-8') for s in range(nsize)]
        cls.parser = redis_parser(cls.redis_py_parser)()
        cls.chunk = cls.parser.multi_bulk(cls.data)

    def test_pack_command(self):
        self.parser.pack_command(self.data)

    def test_encode_multi_bulk(self):
        self.parser.multi_bulk(self.data)

    def test_encode_multi_bulk_bytes(self):
        self.parser.multi_bulk(self.data_bytes)

    def test_decode_multi_bulk(self):
        self.parser.feed(self.chunk)
        self.parser.get()


@unittest.skipUnless(HAS_C_EXTENSIONS, 'Requires C extensions')
class RedisCParser(RedisPyParser):
    redis_py_parser = False
