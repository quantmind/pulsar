import sys

import pulsar


__all__ = ['AsyncTestCaseMixin','test_server']


class test_server(object):
    '''An utility for creating test servers. An instance of this
class should be sent to be run on the arbiter.'''
    def __init__(self, callable, **kwargs):
        self.callable = callable
        self.kwargs = kwargs

    def __call__(self, arbiter):
        cfg = arbiter.get('cfg')
        parse_console = self.kwargs.pop('parse_console',False)
        s = self.callable(parse_console = parse_console,
                          loglevel = cfg.loglevel,
                          **self.kwargs)
        return self.result(s)
    
    def result(self, server):
        return server
    

class CheckFailure(object):
    '''Little utility for asynchronous asserting.'''
    __slots__ = ('test','ExceptionType')
    
    def __init__(self, test, ExceptionType):
        self.test = test
        self.ExceptionType = ExceptionType
        
    def __call__(self, result):
        try:
            self.test.assertTrue(isinstance(result,pulsar.Failure))
            if self.ExceptionType:
                self.test.assertTrue(isinstance(result.trace[1],
                                                self.ExceptionType))
        except:
            return pulsar.Failure(sys.exc_info())
        else:
            return pulsar.CLEAR_ERRORS
        

class AsyncTestCaseMixin(object):
    '''A mixin to use with :class:`unittest.TestCase` classes.'''
    
    def spawn(self, **kwargs):
        ad = pulsar.spawn(**kwargs)
        self.assertTrue(ad.aid)
        self.assertTrue(isinstance(ad, pulsar.ActorProxyDeferred))
        r, outcome = pulsar.async_pair(ad)
        yield r
        a = outcome.result
        self.a = a
        self.assertEqual(a.aid, ad.aid)
    
    def stop(self):
        '''Stop the an actor and check if successful.'''
        arbiter = pulsar.arbiter()
        a = self.a
        yield a.send(arbiter,'stop')
        while a.aid in arbiter.MANAGED_ACTORS:
            yield pulsar.NOT_DONE
        #self.assertFalse(a.is_alive())
        self.assertFalse(a.aid in arbiter.MANAGED_ACTORS)
        
    def assertFailure(self, result, ExceptionType = None):
        '''Asynchronous assert of a :class:`pulsar.Failure`.

:parameter result: the result to check. Can by :class:`pulsar.Deferred` or not.
:parameter ExceptionType: Optional exception type to check.

The usage within a test function is to yield a call to this method.
For example::

    def testMyTestFunction(self):
        ...
        res = ...
        yield self.assertFailure(res,TypeError)
        ...
        
'''
        return pulsar.make_async(result)\
                    .add_callback(CheckFailure(self,ExceptionType))

