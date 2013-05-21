from pulsar.utils.events.saferef import *
from pulsar.apps.test import unittest


class oTest1(object):
    def __call__(self):
        pass

def ftest2(obj):
    pass

class oTest2(object):
    def __call__(self, obj):
        pass


class Tester(unittest.TestCase):
    
    def setUp(self):
        ts = []
        ss = []
        self.closureCount = 0
        for x in range(5000):
            t = oTest1()
            ts.append(t)
            s = safeRef(t, self._closure)
            ss.append(s)
        ts.append(ftest2)
        ss.append(safeRef(ftest2, self._closure))
        for x in range(30):
            t = oTest2()
            ts.append(t)
            s = safeRef(t, self._closure)
            ss.append(s)
        self.ts = ts
        self.ss = ss
    
    def tearDown(self):
        del self.ts
        del self.ss
    
    def testIn(self):
        """Test the "in" operator for safe references (cmp)"""
        for t in self.ts[:50]:
            self.assertTrue(safeRef(t) in self.ss)
    
    def testValid(self):
        """Test that the references are valid (return instance methods)"""
        for s in self.ss:
            self.assertTrue(s())
    
    def testShortCircuit (self):
        """Test that creation short-circuits to reuse existing references"""
        sd = {}
        for s in self.ss:
            sd[s] = 1
        for t in self.ts:
            self.assertTrue(safeRef(t) in sd)
    
    def testRepresentation (self):
        """Test that the reference object's representation works
        
        XXX Doesn't currently check the results, just that no error
            is raised
        """
        repr(self.ss[-1])
        
    def _closure(self, ref):
        """Dumb utility mechanism to increment deletion counter"""
        self.closureCount +=1


