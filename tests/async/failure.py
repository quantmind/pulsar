'''Failure tools.'''
import sys
import gc
from functools import partial

from pulsar import Deferred, Failure, maybe_failure
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, mute_failure, mock

def raise_some_error():
    return 'ciao' + 4

def nested_error():
    yield raise_some_error()

class TestFailure(unittest.TestCase):
    
    def test_traceback(self):
        try:
            yield raise_some_error()
        except Exception:
            failure = Failure(sys.exc_info())
        value = repr(failure)
        self.assertTrue("    return 'ciao' + 4\nTypeError: " in value)
        mute_failure(self, failure)
        
    def test_nested_traceback(self):
        try:
            yield raise_some_error()
        except Exception:
            failure1 = Failure(sys.exc_info())
        try:
            yield nested_error()
        except Exception:
            failure2 = Failure(sys.exc_info())
        self.assertEqual(len(failure1.exc_info[2]), 4)
        self.assertEqual(len(failure2.exc_info[2]), 5)
        self.assertEqual(failure1.exc_info[2][2:], failure2.exc_info[2][3:])
        mute_failure(self, failure1)
        mute_failure(self, failure2)
            
    def test_repr(self):
        failure = maybe_failure(Exception('test_repr'))
        val = str(failure)
        self.assertEqual(repr(failure), val)
        self.assertTrue('Exception: test' in val)
        mute_failure(self, failure)
        
    def testRemote(self):
        failure = maybe_failure(Exception('testRemote'))
        failure.logged = True
        remote = pickle.loads(pickle.dumps(failure))
        self.assertTrue(remote.logged)
        
    def testRemoteExcInfo(self):
        failure = maybe_failure(Exception('testRemoteExcInfo'))
        remote = pickle.loads(pickle.dumps(failure))
        # Now create a failure from the remote.exc_info
        failure2 = maybe_failure(remote.exc_info)
        self.assertEqual(failure2.exc_info, remote.exc_info)
        self.assertFalse(failure.logged)
        self.assertFalse(failure2.logged)
        mute_failure(self, failure)
        mute_failure(self, remote)
        
    def testFailureFromFailure(self):
        failure = maybe_failure(ValueError('test'))
        failure2 = maybe_failure(failure)
        self.assertEqual(failure, failure2)
        self.assertEqual(failure.exc_info, failure2.exc_info)
        failure.logged = True
        self.assertRaises(ValueError, failure.throw)
        
    def testLog(self):
        failure = maybe_failure(Exception('test'))
        error = failure.error
        log = mock.MagicMock(name='log')
        failure.log = log
        del failure
        gc.collect()
        log.assert_called_once_with()