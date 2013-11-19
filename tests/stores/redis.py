import pulsar
from pulsar.apps.test import unittest
from pulsar.apps.data import data_stores

from .pulsardb import RedisCommands


class TestRedisStore(RedisCommands, unittest.TestCase):
    pass


#@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS , 'Requires cython extensions')
#class TestRedisPoolPythonParser(TestRedisStore):
#    pass

