'''Failure tools.'''
import sys
import gc
from functools import partial

from pulsar import Deferred, Failure, maybe_failure
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, mock

def raise_some_error():
    return 'ciao' + 4

def nested_error():
    yield raise_some_error()

class TestTraceBack(unittest.TestCase):
    
    def test_traceback(self):
        try:
            yield raise_some_error()
        except Exception:
            failure = Failure(sys.exc_info())
        value = repr(failure)
        self.assertTrue(("    return 'ciao' + 4\n"
            "TypeError: Can't convert 'int' object to str implicitly") in value)
        
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
        self.assertEqual(len(failure2.exc_info[2]), 6)
        self.assertEqual(failure1.exc_info[2][2:], failure2.exc_info[2][4:])


class f:
#class TestFailure(unittest.TestCase):
        
    def assertRefDeleted(self, error):
        self.assertEqual(self.failure_refs.errors.get(error), 1)
        
    def failure_log(self, failure, log=None, msg=None, level=None):
        failure.logged = True
        
    def dump(self, failure):
        s1 = str(failure)
        log = mock.MagicMock(name='log',
                             side_effect=partial(self.failure_log, failure))
        failure.log = log
        remote = pickle.loads(pickle.dumps(failure))
        self.assertTrue(remote.is_remote)
        self.assertTrue(remote.logged)
        log.assert_called_once_with()
        s2 = str(remote)
        self.assertEqual(s1, s2)
        return remote
    
    def testRepr(self):
        failure = maybe_failure(Exception('test'))
        val = str(failure)
        self.assertEqual(repr(failure), val)
        self.assertTrue('Exception: test' in val)
        
    def testRemote(self):
        failure = maybe_failure(Exception('test'))
        failure.logged = True
        remote = self.dump(failure)
        
    def testRemoteExcInfo(self):
        failure = maybe_failure(Exception('test'))
        remote = self.dump(failure)
        # Now create a failure from the remote.exc_info
        failure = maybe_failure(remote.exc_info)
        self.assertEqual(failure.exc_info, remote.exc_info)
        self.assertTrue(failure.is_remote)
        self.assertTrue(failure.logged)
        
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
        self.assertRefDeleted(error)