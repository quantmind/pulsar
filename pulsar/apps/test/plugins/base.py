import unittest

import pulsar
from pulsar.apps.test.result import Plugin


__all__ = ['WrapTest',
           'TestOption',
           'TestOptionPlugin']

class TestOption(pulsar.Setting):
    virtual = True
    app = 'test'
    section = "Test"


class WrapTest(object):
    '''Wrap an underlying test case'''
    def __init__(self, test):
        self.test = test
        setattr(self, test._testMethodName, self._call)
        self.testMethod = getattr(test, test._testMethodName)

    def __str__(self):
        return self.test._testMethodName
    __repr__ = __str__
    
    @property
    def original_test(self):
        if isinstance(self.test, WrapTest):
            return self.test.original_test
        else:
            return self.test
        
    def set_test_attribute(self, name, value):
        setattr(self.original_test, name, value)
        
    def __getattr__(self, name):
        return getattr(self.original_test, name)
        
    def _call(self):
        # This is the actual function to implement
        return self.testMethod()
    
    
class TestOptionPlugin(Plugin, TestOption):
    '''Base class for test plugins with one option argument.'''
    virtual = True