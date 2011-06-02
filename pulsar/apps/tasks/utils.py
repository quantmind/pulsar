
__all__=  ['SendToQueue','queueTask']


class SendToQueue(object):
    '''Utility class for sending a task to the task queue from a
linked actors used in Http server.
    
.. attribute:: jobname

    The name of job to perform
    
.. attribute:: request

    A Http request instance
    
.. attribute:: server

    The name of the Task Queue Monitor. Default: `"taskqueue"`
    
.. attribute:: request_middleware

    A middleware list for adding extra parameters to the
    :class:`pulsar.apps.tasks.Task` constructor. A request middleware is
    a function which taks two parameters, the :attr:`request` instance and
    a dictionary. For example::
    
        def user_agend_middleware(request,kwargs):
            kwargs['user_agent'] = request.environ.get('HTTP_USER_AGEND','')
            
        SendToQueue.add_request_middleware(user_agend_middleware)
'''
    funcname = {True:'addtask',False:'addtask_noack'}
    request_middleware = []
    
    def __init__(self, jobname, request, server = 'taskqueue', ack = True, **kwargs):
        self.jobname = jobname
        self.request = request
        self.server = server
        self.ack = ack
        self.kwargs = kwargs
        
    def __call__(self, *args, **kwargs):
        request = self.request
        worker = request.environ['pulsar.worker']
        if self.server in worker.ACTOR_LINKS:
            tk = worker.ACTOR_LINKS[self.server]
            kwg = self.kwargs.copy()
            kwg.update(kwargs)
            margs = {}
            for process in self.request_middleware:
                try:
                    process(request,margs)
                except:
                    pass
            targs = (self.jobname,args,kwg)
            name = self.funcname[self.ack] 
            r = tk.send(worker.aid, (targs,margs),name=name, ack=self.ack)
            if self.ack:
                return r
    
    @classmethod
    def add_request_middleware(cls, middleware):
        if middleware not in cls.request_middleware:
            cls.request_middleware.append(middleware)
                
                
def queueTask(jobname, doc = '', ack = True, server = "taskqueue"):
    '''A decorator for sending tasks to the queue. It uses the
Same as :class:`pulsar.apps.tasks.SendToQueue` class.'''
    def _(self, request, *args, **kwargs):
        return SendToQueue(jobname,request,server,ack)(*args,**kwargs)
        
    _.__doc__ = doc
    _.__name__ = jobname
    return _


        

