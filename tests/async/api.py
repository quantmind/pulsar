'''API design'''
import sys
from pulsar.apps.test import unittest, mute_failure
from pulsar import maybe_async, Deferred


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
        self.assertIsInstance(c._result, Failure)
        c._result.mute()

    def test_maybe_async_get_result_false(self):
        a = maybe_async(3, get_result=False)
        self.assertTrue(isinstance(a, Deferred))
        self.assertTrue(a.done())
        self.assertEqual(a.result(), 3)
