import sys
from inspect import isclass
from functools import partial
import threading

import pulsar
from pulsar import is_failure, safe_async, get_actor, send, multi_async


__all__ = ['run_on_arbiter',
           'NOT_TEST_METHODS',
           'ActorTestMixin',
           'AsyncAssert']


class MockArbiter(pulsar.Arbiter):
    '''A mock Arbiter for Testing'''
    def _run(self):
        run = super(MockArbiter, self)._run
        self._test_thread = threading.Thread(name='Mock arbiter thread',
                                             target=run)
        self._test_thread.start()
    

NOT_TEST_METHODS = ('setUp', 'tearDown', '_pre_setup', '_post_teardown',
                    'setUpClass', 'tearDownClass')

class TestCallable:

    def __init__(self, test, method_name, istest, timeout):
        self.test = test
        self.method_name = method_name
        self.istest = istest
        self.timeout = timeout
        
    def __repr__(self):
        if isclass(self.test):
            return '%s.%s' % (self.test.__name__, self.method_name)
        else:
            return '%s.%s' % (self.test.__class__.__name__, self.method_name)
    __str__ = __repr__
    
    def __call__(self, actor):
        test = self.test
        if self.istest:
            test = actor.app.runner.before_test_function_run(test)
        inject_async_assert(test)
        test_function = getattr(test, self.method_name)
        return safe_async(test_function).add_both(partial(self._end, actor))\
                                        .set_timeout(self.timeout)
        
    def _end(self, actor, result):
        if self.istest:
            actor.app.runner.after_test_function_run(self.test, result)
        return result
    

class TestFunction:
    
    def __init__(self, method_name):
        self.method_name = method_name
        self.istest = self.method_name not in NOT_TEST_METHODS
    
    def __repr__(self):
        return self.method_name
    __str__ = __repr__
    
    def __call__(self, test, timeout):
        callable = TestCallable(test, self.method_name, self.istest, timeout)
        return self.run(callable)
        
    def run(self, callable):
        return callable(get_actor())
        
        
class TestFunctionOnArbiter(TestFunction):
    
    def run(self, callable):
        actor = get_actor()
        if actor.is_monitor():
            return callable(actor)
        else:
            # send the callable to the actor monitor
            return actor.send(actor.monitor, 'run', callable)
        
def run_on_arbiter(f):
    '''Decorator for running a test function in the arbiter domain. This
can be useful to test Arbiter mechanics.'''
    f.testfunction = TestFunctionOnArbiter(f.__name__)
    return f
    
    
class AsyncAssertTest(object):
    
    def __init__(self, a, test, name=None):
        self.a = a
        self.test = test
        self.name = name
        
    def __getattr__(self, name):
        return self.__class__(self.a, self.test, name=name)
    
    def __call__(self, *args, **kwargs):
        if self.name == 'assertRaises':
            error, value, args = args[0], args[1], args[2:]
            if hasattr(value, '__call__'):
                value = safe_async(value, *args, **kwargs)
            args = [error, value]
        d = multi_async(args, type=list, raise_on_error=False)
        return d.add_callback(self._check_result)
    
    def _check_result(self, args):
        func = getattr(self.a, self.name, None)
        if func:
            return func(self.test, *args)
        else:
            func = getattr(self.test, self.name)
            return func(*args)
        
        
class AsyncAssert(object):
        
    def __get__(self, instance, instance_type=None):
        return AsyncAssertTest(self, instance)
    
    def assertRaises(self, test, exc_type, value):
        #if is_failure(value):
        #    if not issubclass(exc_type, self.expected):
        #        return False
        def _():
            if is_failure(value):
                value.raise_all()
        test.assertRaises(exc_type, _)
        

class ActorTestMixin(object):
    '''A mixin for testing spawning of actors. Make sure this
is the first class you derive from, before the unittest.TestCase, so that
the tearDown method is overwritten.'''
    concurrency = 'thread'
    
    @property
    def all_spawned(self):
        if not hasattr(self, '_spawned'):
            self._spawned = []
        return self._spawned
        
    def spawn(self, concurrency=None, **kwargs):
        '''Spawn a new actor and perform some tests.'''
        concurrency = concurrency or self.concurrency
        ad = pulsar.spawn(concurrency=concurrency, **kwargs)
        self.assertTrue(ad.aid)
        self.assertTrue(isinstance(ad, pulsar.ActorProxyDeferred))
        yield ad
        proxy = ad.result
        self.all_spawned.append(proxy)
        self.assertEqual(proxy.aid, ad.aid)
        self.assertEqual(proxy.proxy, proxy)
        self.assertTrue(proxy.cfg)
        yield proxy
    
    def stop_actors(self, *args):
        all = args or self.all_spawned
        if len(all) == 1:
            return send(all[0], 'stop')
        elif all:
            return multi_async((send(a, 'stop') for a in all))
            
    def tearDown(self):
        return self.stop_actors()
        
        
def inject_async_assert(obj):
    tcls = obj if isclass(obj) else obj.__class__
    if not hasattr(tcls, 'async'):
        tcls.async = AsyncAssert()

