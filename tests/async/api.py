'''API design'''
import sys
from pulsar.apps.test import unittest
from pulsar import NOT_DONE, Failure


class Context(object):
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        if type:
            self.result = Failure((type, value, traceback))
        else:
            self.result = None
        return True
            

class TestApi(unittest.TestCase):
    
    def test_with_statement(self):
        with Context() as c:
            yield NOT_DONE
            yield NOT_DONE
            raise ValueError
        self.assertTrue(c.result)