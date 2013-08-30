import gc
from inspect import isclass
from functools import partial
import threading

import pulsar
from pulsar import safe_async, get_actor, send, multi_async

from .case import get_stream

__all__ = ['run_on_arbiter',
           'NOT_TEST_METHODS',
           'ActorTestMixin',
           'AsyncAssert',
           'show_leaks']


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
    '''Decorator for running a test function in the :class:`pulsar.Arbiter`
context domain. This can be useful to test Arbiter mechanics.'''
    f.testfunction = TestFunctionOnArbiter(f.__name__)
    return f
    
    
class AsyncAssert(object):
    '''A `descriptor`_ which the :ref:`test-suite` add to all python
:class:`unitest.TestCase`. It can be used to invoke the same
``assertXXX`` methods available in the :class:`unitest.TestCase` with the
added bonus they it waorks for asynchronous results too.

The descriptor is available bia the ``async`` attribute. For example::

    class MyTest(unittest.TestCase):
    
        def test1(self):
            yield self.async.assertEqual(3, Deferred().callback(3))
          
    
.. _descriptor: http://users.rcn.com/python/download/Descriptor.htm'''
    def __init__(self, test=None):
        self.test = test
    
    def __get__(self, instance, instance_type=None):
        return AsyncAssert(instance)
        
    def __getattr__(self, name):
        def _(*args, **kwargs):
            args = yield multi_async(args)
            yield getattr(self.test, name)(*args, **kwargs)
        return _
        
    def assertRaises(self, error, callable, *args, **kwargs):
        try:
            yield callable(*args, **kwargs)
        except error:
            pass
        except Exception:
            raise self.test.failureException('%s not raised by %s'
                                             % (error, callable))
        else:
            raise self.test.failureException('%s not raised by %s'
                                             % (error, callable))
        

class ActorTestMixin(object):
    '''A mixin for :class:`unittest.TestCase`.
    
Useful for classes testing spawning of actors.
Make sure this is the first class you derive from, before the
unittest.TestCase, so that the tearDown method is overwritten.

.. attribute:: concurrency

    The concurrency model used to spawn actors via the :meth:`spawn`
    method.
'''
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


def show_leaks(actor):
    '''Function to show memory leaks on a processed-based actor.'''
    if not actor.is_process():
        return
    gc.collect()
    if gc.garbage:
        MAX_SHOW = 100
        stream = get_stream(actor.cfg)
        stream.writeln('MEMORY LEAKS REPORT IN %s' % actor)
        stream.writeln('Created %s uncollectable objects' % len(gc.garbage))
        for obj in gc.garbage[:MAX_SHOW]:
            stream.writeln('Type: %s' % type(obj))
            stream.writeln('=================================================')
            stream.writeln('%s' % obj)
            stream.writeln('-------------------------------------------------')
            stream.writeln('')
            stream.writeln('')
        if len(gc.garbage) > MAX_SHOW:
            stream.writeln('And %d more' % (len(gc.garbage) - MAX_SHOW))