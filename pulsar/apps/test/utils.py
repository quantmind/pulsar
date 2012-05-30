import sys
from inspect import isclass

import pulsar
from pulsar import is_failure
from pulsar.async import commands
from pulsar.async.defer import pickle


__all__ = ['create_test_arbiter',
           'run_on_arbiter',
           'halt_server',
           'arbiter_test',
           'ActorTestMixin',
           'AsyncAssert',
           'test_server']


class MockArbiter(pulsar.Arbiter):
    '''A mock Arbiter for Testing'''
    def _run(self):
        run = super(MockArbiter, self)._run
        self._test_thread = threading.Thread(name='Mock arbiter thread',
                                             target=run)
        self._test_thread.start()
    

class ObjectMethod:
    
    def __init__(self, obj, method):
        self.test = obj
        self.method = method
        
    def __call__(self, actor):
        test = self.test
        test_function = getattr(test, self.method)
        return test_function()
    
        
def create_test_arbiter(test=True):
    '''Create an instance of MockArbiter for testing'''
    commands_set = set(commands.actor_commands)
    commands_set.update(commands.arbiter_commands)
    actor_maker = pulsar.concurrency('monitor', MockArbiter, 1000,
                                     None, 'arbiter', commands_set,
                                     {'__test_arbiter__': test})
    arbiter = actor_maker.actor
    arbiter.start()
    return arbiter
    
def halt_server(exception=None):
    exception = exception or pulsar.HaltServer('testing')
    raise exception
    
def run_on_arbiter(f):
    '''Decorator for running a test function in the arbiter domain. This
can be usefull to test Arbiter mechanics.'''
    name = f.__name__
    def _(obj):
        actor = pulsar.get_actor()
        if actor.is_arbiter():
            # In the arbiter, simply execute the function
            # First inject the AsyncAssert instance
            inject_async_assert(obj)
            return pulsar.safe_async(f, args=(obj,))
        else:
            # send the test case to the arbiter.
            # At some point we will get a result or an error back in
            # the test worker arbiter inbox.
            callable = ObjectMethod(obj, name)
            msg = actor.send('arbiter', 'run', callable)
            return msg
    _.__name__ = name
    _.__doc__ = f.__doc__
    return _
    
def arbiter_test(f):
    '''Decorator for testing arbiter mechanics. It creates a mock arbiter
running on a separate thread and run the tet function on the arbiter thread.'''
    @pulsar.async
    def work(self):
        outcome = pulsar.safe_async(f, args= (self,))
        yield outcome
        yield self.arbiter.stop()
        self.d.callback(outcome.result)
        
    def _(self):
        self.arbiter = create_test_arbiter()
        self.d = pulsar.Deferred()
        self.arbiter.ioloop.add_callback(lambda: work(self))
        yield self.d
    
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _


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

