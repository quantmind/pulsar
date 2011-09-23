import unittest


class TestCase(unittest.TestCase):
    '''A specialised test case which offers three
additional functions: i) `initTest` and ii) `endTests`,
called at the beginning and at the end of all tests functions declared
in derived classes. Useful for starting a server to send requests
to during tests. iii) `runInProcess` to run a
callable in the main process.'''
    suiterunner = None
    
    def __init__(self, methodName=None):
        if methodName:
            self._dummy = False
            super(TestCase,self).__init__(methodName)
        else:
            self._dummy = True
    
    def __repr__(self):
        if self._dummy:
            return self.__class__.__name__
        else:
            return super(TestCase,self).__repr__()
    
    def initTests(self):
        '''Called at the beginning off all tests functions in the class'''
        pass
    
    def endTests(self):
        '''Called at the end off all tests functions in the class'''
        pass
    
    @property    
    def arbiter(self):
        return pulsar.arbiter()
        
    def sleep(self, timeout):
        time.sleep(timeout)
        
    def Callback(self):
        return TestCbk()

    def initTests(self):
        pass
    
    def endTests(self):
        pass
    
    def stop(self, a):
        '''Stop an actor and wait for the exit'''
        a.stop()
        still_there = lambda : a.aid in self.arbiter.LIVE_ACTORS
        self.wait(still_there)
        self.assertFalse(still_there())
        
    def wait(self, callback, timeout = 5):
        t = time.time()
        while callback():
            if time.time() - t > timeout:
                break
            self.sleep(0.1)
    
    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
            startTestRun = getattr(result, 'startTestRun', None)
            if startTestRun is not None:
                startTestRun()

        self._resultForDoCleanups = result
        result.startTest(self)
        if getattr(self.__class__, "__unittest_skip__", False):
            # If the whole class was skipped.
            try:
                result.addSkip(self, self.__class__.__unittest_skip_why__)
            finally:
                result.stopTest(self)
            return
        testMethod = getattr(self, self._testMethodName)
        TestGenerator(self, result, testMethod)()
        
        