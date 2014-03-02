'''API design'''
import sys
import unittest

from pulsar import maybe_async


class Context(object):

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type:
            self._result = Failure((type, value, traceback))
        else:
            self._result = None
        return True


class TestApi(unittest.TestCase):

    def test_with_statement(self):
        with Context() as c:
            yield None
            yield None
            raise ValueError
        self.assertIsInstance(c._result, ValueError)
