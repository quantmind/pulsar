from collections import Mapping

import pulsar
from pulsar.apps import wsgi
from pulsar.apps import rpc

from .exceptions import TaskNotAvailable
from .task import Task


__all__ = ['TaskQueueRpcMixin']


def task_to_json(task):
    if task:
        if pulsar.is_failure(task):
            err = task.trace[1]
            if isinstance(err, TaskNotAvailable):
                raise rpc.InvalidParams('Job "%s" is not available.'\
                                        % err.task_name)
        if isinstance(task, (list, tuple)):
            task = [task_to_json(t) for t in task]
        elif isinstance(task, Task):
            task = task.tojson()
    return task
    

class TaskQueueRpcMixin(rpc.JSONRPC):
    '''A :class:`pulsar.apps.rpc.JSONRPC` mixin for communicating with
a :class:`TaskQueue`.
To use it, you need to have an :ref:`RPC application <apps-rpc>`
and a :ref:`task queue <apps-tasks>` application installed in the
:class:`pulsar.Arbiter`.

:parameter taskqueue: instance or name of the
    :class:`pulsar.apps.tasks.TaskQueue` which exposes the remote procedure
    calls.
    

**Remote Procedure Calls**

.. method:: job_list([jobnames=None])

    Return the list of :class:`Job` registered with task queue with meta
    information. If a list of jobnames is given, it returns only jobs
    included in the list.
    
    :rtype: A list of dictionaries
    
    
.. method:: run_new_task(jobname, [**kwargs])
    
    Run a new :class:`Task` in the task queue. The task can be of any type
    as long as it is registered in the :class:`Job` registry.

    :parameter jobname: the name of the :class:`Job` to run.
    :parameter kwargs: optional key-valued job parameters.
    :rtype: a dictionary containing information about the
        :class:`Task` submitted
    
    
.. method:: get_task(id=task_id)

    Retrieve a task from its ``id``.
    Returns ``None`` if the task is not available.
    
    
.. method:: get_tasks(**filters)

    Retrieve a list of tasks which satisfy *filters*.
    
    
.. method:: wait_for_task(id=task_id)

    Wait for a task to have finished.
'''
    def __init__(self, taskqueue, **kwargs):
        if not isinstance(taskqueue, str):
            taskqueue = taskqueue.name
        self.taskqueue = taskqueue
        super(TaskQueueRpcMixin,self).__init__(**kwargs)
        
    ############################################################################
    ##    REMOTES
    def rpc_job_list(self, request, jobnames=None):
        return self._rq(request, 'job_list', jobnames=jobnames)
    
    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self._rq(request, 'next_scheduled', jobnames=jobnames)
        
    def rpc_run_new_task(self, request, jobname=None, **kw):
        result = self.run_new_task(request, jobname, **kw)
        return result.addBoth(task_to_json)
        
    def rpc_get_task(self, request, id=None):
        if id:
            return self._rq(request, 'get_task', id).add_callback(task_to_json)
    
    def rpc_get_tasks(self, request, **params):
        if params:
            return self._rq(request, 'get_tasks', **params).add_callback(task_to_json)
        
    def rpc_wait_for_task(self, request, id=None):
        if id:
            return self._rq(request, 'wait_for_task', id).add_callback(task_to_json)
    
    ############################################################################
    ##    INTERNALS
    def run_new_task(self, request, jobname, args=None,
                     ack=True, meta_data=None, **kw):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        funcname = 'addtask' if ack else 'addtask_noack'
        meta_data = meta_data or {}
        meta_data.update(self.task_request_parameters(request))
        args = args or ()
        return self._rq(request, funcname, jobname, meta_data, *args, **kw)
    
    def task_request_parameters(self, request):
        '''**Internal function** which returns a dictionary of parameters
to be passed to the :class:`Task` class constructor.
This function can be overridden to add information about
the type of request, who made the request and so forth. It must return
a dictionary. By default it returns an empty dictionary.'''
        return {}
    
    def _rq(self, request, action, *args, **kw):
        return pulsar.send(self.taskqueue, action, *args, **kw)
    
