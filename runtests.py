#!/usr/bin/env python
import os
import sys
from pulsar.apps.test import TestSuiteRunner


class TestExtractor(object):
    TESTMAPPING = {'regression':'tests','bench':'bench','profile':'profile'}
    def __init__(self, path):
        self.path = path
        
    def testdir(self, testtype):
        return os.path.join(self.path,testtype)
    
    def test_module(self, testtype, loc, app):
        return '{0}.{1}.tests'.format(loc,app)
        
        
class ExampleExtractor(TestExtractor):
    
    def testdir(self, testtype):
        return self.path
    
    def test_module(self, testtype, loc, app):
        name = self.TESTMAPPING[testtype]
        return '{0}.{1}.{2}'.format(loc,app,name)

    
def run():
    '''To perform preprocessing before tests add a cfg.py module'''
    dirs = (('examples',ExampleExtractor),
            ('tests',TestExtractor))
    try:
        import cfg
    except ImportError:
        pass
    p = lambda x : os.path.split(x)[0]
    path = p(os.path.abspath(__file__))
    
    running_tests = []
    for t,c in dirs:
        p = os.path.join(path,t)
        if p not in sys.path:
            sys.path.insert(0, p)
        running_tests.append(c(p))
        
    TestSuiteRunner(running_tests).start()

if __name__ == '__main__':
    run()
