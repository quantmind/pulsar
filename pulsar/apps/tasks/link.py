import functools

from pulsar.net import Remotecall, HttpActorManager

from .exceptions import TaskQueueException


__all__=  ['HttpTaskManager','SendToQueue','queueTask','nice_task_message']


format_time = lambda x : x
        

class SendToQueue(Remotecall):
    '''A specialized :class:`pulsar.http.Remotecall` class
which facilitates the interaction between a
:class:`pulsar.apps.tasks.TaskQueue` application and a
:class:`pulsar.apps.wsgi.WSGIApplication` application running on the
same Arbiter.

Used for sending a job request to the task queue from an actors
serving a Http request.
    
.. attribute:: jobname

    The name of job to perform in the task queue
    
Usually this class is invoked by an instance of a
:class:`pulsar.apps.tasks.HttpTaskManager`.
'''
    funcname = {True:'addtask',False:'addtask_noack'}
    
    def __init__(self, manager, request,  jobname, ack = True, **kwargs):
        remotefunction = self.funcname[ack]
        self.jobname = jobname
        super(SendToQueue,self).__init__(manager, request, remotefunction,
                                         ack = ack, **kwargs)
        
    def get_args(self, request, args, kwg):
        margs = self.manager.process_middleware(request)
        return ((self.jobname,args,kwg),margs)
        

class HttpTaskManager(HttpActorManager):
    '''A specialized :class:`pulsar.http.HttpActorManager` class for requesting
task in a taskqueue application.'''
    def maketask(self, request, jobname, ack = True, **kwargs):
        return SendToQueue(self,request,jobname,ack,**kwargs)
    
                
class queueTask(object):
    '''A class for sending tasks of a specific job to the queue.
In a general case you would simply use the
:meth:`pulsar.apps.tasks.HttpTaskManager.maketask` function.
This decorator can be used on a rpc server class so that it exposes a job name
as an rpc function.'''
    def __init__(self, jobname, doc = '', ack = True,
                 manager = None, **kwargs):
        self.manager = manager
        self.__doc__ = doc
        self.__name__ = jobname
        self.ack = ack
        self.kwargs = kwargs
        
    def __call__(self, request, manager = None, **kwargs):
        manager = manager or self.manager
        if manager:
            return manager.maketask(request,self.__name__,request,ack)\
                                    (*args,**kwargs)
        else:
            raise TaskQueueException('Task manager not specified')

    def __get__(self, obj, objtype):
        """Support instance methods."""
        manager = getattr(obj,'task_queue_manager',None)
        return functools.partial(self.__call__,manager = manager)


def nice_task_message(req, smart_time = None):
    smart_time = smart_time or format_time
    status = req['status'].lower()
    user = req.get('user',None)
    ti = req.get('time_start',req.get('time_executed',None))
    name = '{0} ({1}) '.format(req['name'],req['id'][:8])
    msg = '{0} {1} at {2}'.format(name,status,smart_time(ti))
    if user:
        msg = '{0} by {1}'.format(msg,user)
    return msg

