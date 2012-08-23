from collections import Mapping

import pulsar
from pulsar.apps import wsgi
from pulsar.apps import rpc

from .exceptions import TaskNotAvailable


__all__ = ['TaskQueueRpcMixin']


def task_to_json(task):
    if task:
        if pulsar.is_failure(task):
            err = task.trace[1]
            if isinstance(err, TaskNotAvailable):
                raise rpc.InvalidParams('Job "%s" is not available.'\
                                        % err.task_name)
        else:
            task = task.tojson()
    return task
    

class TaskQueueRpcMixin(rpc.JSONRPC):
    '''A :class:`pulsar.apps.rpc.JSONRPC` mixin for communicating with
a :class:`TaskQueue`.
To use it, you need to have an :ref:`RPC application <apps-rpc>`
and a :ref:`task queue <apps-tasks>` application installed in the
:class:`pulsar.Arbiter`.

:parameter taskqueue: set the :attr:`task_queue_manager` attribute. It can be
    a :class:`pulsar.apps.tasks.TaskQueue` instance or a name of a taskqueue.

.. attribute:: task_queue_manager

    A :class:`pulsar.ActorLink` for facilitating the communication
    from the rpc workers to the task queue.
    
It exposes the following remote functions:

.. method:: job_list([jobnames=None])

    Return the list of jobs registered with task queue with meta information.
    If a list of jobnames is given, it returns only jobs included in the list.
    
    :rtype: A list of dictionaries
    
    
.. method:: run_new_task(jobname, [**kwargs])
    
    Run a new task in the task queue. The task can be of any type
    as long as it is registered in the job registry.

    :parameter jobname: the name of the job to run.
    :parameter kwargs: optional key-valued job parameters.
    :rtype: a dictionary containing information about the request
    
    
.. method:: get_task(task_id)

    Retrieve a task from its ``id``.
    Returns ``None`` if the task is not available.
'''
    def __init__(self, taskqueue, **kwargs):
        if not isinstance(taskqueue, str):
            taskqueue = taskqueue.name
        self.task_queue_manager = wsgi.WsgiActorLink(taskqueue)
        super(TaskQueueRpcMixin,self).__init__(**kwargs)
        
    ############################################################################
    ##    REMOTES
    def rpc_job_list(self, request, jobnames=None):
        return self.task_queue_manager(request.environ,
                                       'job_list',
                                       jobnames=jobnames)
    
    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self.task_queue_manager(request.environ,
                                       'next_scheduled',
                                       jobnames=jobnames)
        
    def rpc_run_new_task(self, request, jobname=None, ack=True,
                         meta_data=None, **kwargs):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        result = self.task_callback(request, jobname, ack, meta_data, **kwargs)
        return result().addBoth(task_to_json)
        
    def rpc_get_task(self, request, id=None):
        if id:
            return self.task_queue_manager(request.environ,
                                           'get_task',
                                           id).add_callback(task_to_json)
    
    ############################################################################
    ##    INTERNALS
    def task_request_parameters(self, request):
        '''Internal function which returns a dictionary of parameters
to be passed to the :class:`Task` class constructor.

This function can be overridden to add information about
the type of request, who made the request and so forth. It must return
a dictionary and it is called by the internal
:meth:`task_callback` method.

By default it returns an empty dictionary.'''
        return {}
    
    def task_callback(self, request, jobname, ack=True, meta_data=None,
                      **kwargs):
        '''Internal function which uses the :attr:`task_queue_manager`
to create an :class:`pulsar.ActorLinkCallback` for running a task
from *jobname* in the :class:`TaskQueue`.
The function returned by this method can be called with exactly
the same ``args`` and ``kwargs`` as the callable method of the :class:`Job`
(excluding the ``consumer``).'''
        funcname = 'addtask' if ack else 'addtask_noack'
        request_params = self.task_request_parameters(request)
        if isinstance(meta_data, Mapping):
            request_params.update(meta_data)
        return self.task_queue_manager.create_callback(
                                            request.environ,
                                            funcname,
                                            jobname,
                                            request_params,
                                            **kwargs)