from pulsar.async.defer import Deferred, AlreadyCalledError


class Remotecall(object):
    '''\
An utility function for sending a remote call to a manager
from an actor serving an Http request. An instance of this class
can be called once only, otherwise an error occurs.

:parameter manager: a :class:`pulsar.http.HttpActorManager` instance
                    handling the communication.
:parameter request: a http request object. It contains the environment.
:parameter remotefunction: The function to invoke in the remote actor.
                          The remote actor must have a
                          "actor_<remotefunction>" function defined.
:parameter kwargs: disctionary of default remote function key-valued arguments.
'''
    def __init__(self, manager, request, remotefunction, ack = True, **kwargs):
        super(Remotecall,self).__init__()
        self.manager = manager
        self.request = request
        self.remotefunction = remotefunction
        self.kwargs = kwargs
        self.ack = ack
        
    def get_args(self, request, args, kwg):
        return (args,kwg)
        
    def __call__(self, *args, **kwargs):
        '''\
Send the actual request to the remote manager and return the deferred request.
        
:parameter args: tuple of remote function arguments
:parameter kwargs: dictionary of remote function key-valued arguments'''
        if hasattr(self,'_result'):
            raise AlreadyCalledError()
        self._result = None
        request = self.request
        manager = self.manager
        worker = request.environ['pulsar.worker']
        if manager.server in worker.ACTOR_LINKS:
            tk = worker.ACTOR_LINKS[manager.server]
            kwg = self.kwargs.copy()
            kwg.update(kwargs)
            params = self.get_args(request, args, kwg)
            self._result = tk.send(worker.aid,
                                   params,
                                   name=self.remotefunction,
                                   ack = self.ack)
            worker.wake_arbiter()
        else:
            self._result = Deferred()
            try:
                raise ValueError('Got a request from manager {0} which is\
 not in actor links'.format(manager.server))
            except Exceptions as e:
                self._result.callback(e)
            
        return self._result
    
    def result(self):
        if not hasattr(self,'_result'):
            self._result = Deferred().callback(None)
        return self._result
    
        
class HttpActorManager(object):
    '''\
A manager for a remote Actor acting as a server.

.. attribute:: server

    The name of the remote actor acting as a sever
    
.. attribute:: request_middleware

    A middleware list for adding extra parameters to the
    :class:`pulsar.apps.tasks.Task` constructor. A request middleware is
    a function which taks two parameters, the :attr:`request` instance and
    a dictionary. For example::
    
        def user_agend_middleware(request,kwargs):
            kwargs['user_agent'] = request.environ.get('HTTP_USER_AGEND','')
            
        SendToQueue.add_request_middleware(user_agend_middleware)
'''
    RequestClass = Remotecall
    
    def __init__(self, server, middleware = None):
        self.server = server
        self.request_middleware = middleware or []
        
    def add_request_middleware(self, middleware):
        '''Add a middleware function to the request_middleware list.
A request middleware function taks two parameters: the request object
and a dictionary of key-value parameter to be send to the task function.'''
        if middleware not in self.request_middleware:
            self.request_middleware.append(middleware)
    
    def __call__(self, request, remotefunction, *args, **kwargs):
        return self.make(request, remotefunction, **kwargs)(*args)
        
    def make(self, request, remotefunction, **kwargs):
        return self.RequestClass(self, request, remotefunction, **kwargs)

    def process_middleware(self, request):
        margs = {}
        for process in self.request_middleware:
            try:
                process(request,margs)
            except:
                pass
        return margs

