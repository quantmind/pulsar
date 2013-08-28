'''Failure tools.'''
import sys
import gc
from functools import partial

from pulsar import Deferred, Failure, maybe_failure
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, mock

def _getnone():
    return None

def passthrough(*args, **kw):
    pass


class PickableMock(mock.MagicMock):
 
    def __reduce__(self):
        return (_getnone,())


class TestFailure(unittest.TestCase):
    
    def failure_log(self, failure, log=None, msg=None, level=None):
        failure.logged = True
        
    def dump(self, failure):
        s1 = str(failure)
        log = PickableMock(name='log',
                           side_effect=partial(self.failure_log, failure))
        failure.log = log
        remote = pickle.loads(pickle.dumps(failure))
        self.assertEqual(remote.log, None)
        self.assertTrue(remote.is_remote)
        self.assertTrue(remote.logged)
        log.asser_called_once_with()
        s2 = str(remote)
        self.assertEqual(s1, s2)
        remote.log = passthrough
        return log, remote
        
    def testRepr(self):
        failure = Failure.make(Exception('test'))
        val = str(failure)
        self.assertEqual(repr(failure), val)
        self.assertTrue('Exception: test' in val)
        
    def testRemote(self):
        failure = Failure.make(Exception('test'))
        failure.logged = True
        log, remote = self.dump(failure)
        
    def testRemoteExcInfo(self):
        failure = Failure.make(Exception('test'))
        log, remote = self.dump(failure)
        # Now create a failure from the remote.exc_info
        failure = maybe_failure(remote.exc_info)
        self.assertEqual(failure.exc_info, remote.exc_info)
        self.assertTrue(failure.is_remote)
        self.assertTrue(failure.logged)
        
    def testFailureFromFailure(self):
        failure = Failure.make(ValueError('test'))
        failure2 = Failure.make(failure)
        self.assertNotEqual(failure, failure2)
        self.assertEqual(failure.exc_info, failure2.exc_info)
        failure.logged = True
        self.assertRaises(ValueError, failure.throw)
        
    def testLog(self):
        failure = Failure.make(ValueError('test log'))
        log = mock.MagicMock(name='log')
        failure.log = log
        s = gc.get_referrers(failure)
        del failure
        gc.collect()
        log.asser_called_once_with(msg='Deferred Failure never retrieved')