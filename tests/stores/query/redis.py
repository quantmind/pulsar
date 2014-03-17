import unittest

from . import QueryTest
from ..redis import RedisDbTest


class TestCouchdbQuery(RedisDbTest, QueryTest, unittest.TestCase):
    pass
