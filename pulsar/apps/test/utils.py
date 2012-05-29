import sys

import pulsar
from pulsar.async import commands
from pulsar.async.defer import pickle


__all__ = ['create_test_arbiter',
           'run_on_arbiter',
           'halt_server',
           'arbiter_test',
           'AsyncTestCaseMixin',
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
    '''Decorator for running a test function in the arbiter domain'''
    name = f.__name__
    def _(obj):
        actor = pulsar.get_actor()
        if actor.is_arbiter():
            return pulsar.safe_async(f, args=(obj,))
        else:
            callable = ObjectMethod(obj, name)
            return actor.send('arbiter', 'run', callable)
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
    

class AsyncAssert(object):
    __slots__ = ('test', 'name')
    
    def __init__(self, test=None, name=None):
        self.test = test
        self.name = name
    
    def __get__(self, instance, instance_type=None):
        return self.__class__(test=instance)
            
    def __getattr__(self, name):
        return self.__class__(test=self.test, name=name)
    
    def __call__(self, elem, *args):
        return make_async(elem).add_callback(\
                        lambda r : self._check_result(r,*args))
    
    def _check_result(self, result, *args):
        func = getattr(self.test, self.name)
        return func(result, *args)
    
    def __reduce__(self):
        return (self.__class__,())
            

class AsyncTestCaseMixin(object):
    '''A mixin to use with :class:`unittest.TestCase` classes.'''
    a = None
    def spawn(self, **kwargs):
        '''Spawn an actor and store its proxy in the as "a" attribute.
To use, do a yeild::

    yield self.start()
    
'''
        ad = pulsar.spawn(**kwargs)
        self.assertTrue(ad.aid)
        self.assertTrue(isinstance(ad, pulsar.ActorProxyDeferred))
        yield ad
        a = ad.result
        self.a = a
        self.assertEqual(a.aid, ad.aid)
    
    def stop(self):
        '''Stop the an actor and check if successful.'''
        if self.a:
            arbiter = pulsar.arbiter()
            a = self.a
            yield a.send(arbiter,'stop')
            while a.aid in arbiter.MANAGED_ACTORS:
                yield pulsar.NOT_DONE
            #self.assertFalse(a.is_alive())
            self.assertFalse(a.aid in arbiter.MANAGED_ACTORS)
        


