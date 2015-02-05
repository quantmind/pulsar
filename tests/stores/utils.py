import re
import unittest

from pulsar.apps.ds import redis_to_py_pattern


class TestUtils(unittest.TestCase):

    def match(self, c, text):
        self.assertEqual(c.match(text).group(), text)

    def not_match(self, c, text):
        self.assertEqual(c.match(text), None)

    def test_redis_to_py_pattern(self):
        p = redis_to_py_pattern('h?llo')
        c = re.compile(p)
        self.match(c, 'hello')
        self.match(c, 'hallo')
        self.not_match(c, 'haallo')
        self.not_match(c, 'hallox')
        #
        p = redis_to_py_pattern('h*llo')
        c = re.compile(p)
        self.match(c, 'hello')
        self.match(c, 'hallo')
        self.match(c, 'hasjdbvhckjcvkfcdfllo')
        self.not_match(c, 'haallox')
        self.not_match(c, 'halloouih')
        #
        p = redis_to_py_pattern('h[ae]llo')
        c = re.compile(p)
        self.match(c, 'hello')
        self.match(c, 'hallo')
        self.not_match(c, 'hollo')
