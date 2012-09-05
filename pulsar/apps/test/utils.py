import sys
from inspect import isclass
import threading

import pulsar
from pulsar import is_failure, async, get_actor
from pulsar.async import commands
from pulsar.async.defer import pickle


__all__ = ['create_test_arbiter',
           'run_on_arbiter',
           'run_test_function',
           'halt_server',
           'arbiter_test',
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

class ObjectMethod:
    
    def __init__(self, test, method_name):
        self.test = test
        self.method_name = method_name
        
    @async(max_errors=1, description='Test ')
    def __call__(self, actor):
        test = self.test
        if self.method_name not in NOT_TEST_METHODS:
            worker = get_actor()
            runner = worker.app.runner
            test = runner.getTest(test)
        test_function = getattr(test, self.method_name)
        return test_function()
    
        
def create_test_arbiter(test=True):
    '''Create an instance of MockArbiter for testing'''
    commands_set = set(commands.actor_commands)
    commands_set.update(commands.arbiter_commands)
    arbiter = pulsar.concurrency('monitor', MockArbiter, 1000,
                                 None, 'arbiter', commands_set,
                                 {'__test_arbiter__': test})
    arbiter.start()
    return arbiter
    
def halt_server(exception=None):
    exception = exception or pulsar.HaltServer('testing')
    raise exception
    
def run_on_arbiter(f):
    '''Decorator for running a test function in the arbiter domain. This
can be useful to test Arbiter mechanics.'''
    name = f.__name__
    def _(obj):
        callable = ObjectMethod(obj, name)
        actor = get_actor()
        if actor.is_monitor():
            # In the test monitor, simply execute the function
            # First inject the AsyncAssert instance
            inject_async_assert(obj)
            return callable(actor)
        else:
            return actor.send(actor.monitor, 'run', callable)
    _.test_function = True
    _.__name__ = name
    _.__doc__ = f.__doc__
    return _


def run_test_function(test, func):
    '''Run function *func* which belong to *test*.

:parameter test: test instance or class
:parameter func: test function belonging to *test*
:return: an asynchronous result
'''
    if func is None:
        return func
    if not getattr(func, 'test_function', False):
        func = ObjectMethod(test, func.__name__)
    return func(get_actor())

    
def arbiter_test(f):
    '''Decorator for testing arbiter mechanics. It creates a mock arbiter
running on a separate thread and run the tet function on the arbiter thread.'''
    d = pulsar.Deferred()
    def work(self):
        yield f(self)
        yield self.arbiter.stop()
    @pulsar.async
    def safe(self):
        yield work(self)
        d.callback(True)
    def _(self):
        self.arbiter = create_test_arbiter()
        while not self.arbiter.started:
            yield pulsar.NOT_DONE
        self.arbiter.ioloop.add_callback(lambda: safe(self))
        yield d
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _
    
    
class AsyncAssertTest(object):
    
    def __init__(self, a, test, name=None):
        self.a = a
        self.test = test
        self.name = name
        
    def __getattr__(self, name):
        return self.__class__(self.a, self.test, name=name)
    
    def __call__(self, *args):
        d = pulsar.MultiDeferred(args, type=list).lock()
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
    
    def assertRaises(self, test, excClass, value):
        def _():
            if is_failure(value):
                value.raise_all()
        test.assertRaises(excClass, _)
        

class ActorTestMixin(object):
    '''A mixin for testing spawning of actors. Make sure this
is the first class you derive from, before the unittest.TestCase, so that
the tearDown method is overwritten.'''
    concurrency = 'thread'
    a = None
    
    @property
    def all_spawned(self):
        if not hasattr(self, '_spawned'):
            self._spawned = []
        return self._spawned
        
    def spawn(self, concurrency=None, **kwargs):
        concurrency = concurrency or self.concurrency
        ad = pulsar.spawn(concurrency=concurrency,**kwargs)
        self.assertTrue(ad.aid)
        self.assertTrue(isinstance(ad, pulsar.ActorProxyDeferred))
        yield ad
        self.a = ad.result
        self.all_spawned.append(self.a)
        self.assertEqual(self.a.aid, ad.aid)
        self.assertTrue(self.a.address)
    
    def stop_actors(self, *args):
        all = args or self.all_spawned
        if len(all) == 1:
            return all[0].stop()
        elif all:
            return MultiDeferred((a.stop() for a in all)).lock()
            
    def tearDown(self):
        return self.stop_actors()
        
        
def inject_async_assert(obj):
    tcls = obj if isclass(obj) else obj.__class__
    if not hasattr(tcls,'async'):
        tcls.async = AsyncAssert()

