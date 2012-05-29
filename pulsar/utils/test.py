import sys
import threading

if sys.version_info >= (2,7):
    import unittest as test
else:
    try:
        import unittest2 as test
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unitest2 package')
        exit(0)

import pulsar
from pulsar.async import commands
from pulsar.async.defer import pickle

class MockArbiter(pulsar.Arbiter):
    '''A mock Arbiter for Testing'''
    def _run(self):
        run = super(MockArbiter, self)._run
        self._test_thread = threading.Thread(name='Mock arbiter thread',
                                             target=run)
        self._test_thread.start()
    
        
    
def create_test_arbiter(test=True):
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
    
class ObjectMethod:
    
    def __init__(self, obj, method):
        if hasattr(obj,'async'):
            delattr(obj,'async')
        self.test = pickle.dumps(obj)
        self.istest = obj.istest
        self.method = method
        
    def __call__(self, actor):
        test = pickle.loads(self.test)
        if self.istest:
            worker = actor.monitors['test']
            test = worker.app.runner.getTest(test)
        test_function = getattr(test, self.method)
        return test_function()
    
def run_on_arbiter(f):
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
