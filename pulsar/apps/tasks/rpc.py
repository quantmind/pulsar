'''
The :ref:`task queue application <app-taskqueue-app>` does not expose
an external API to run new tasks or retrieve task information.
The :class:`TaskQueueRpcMixin` class can be used to achieve just that. It is
a :class:`pulsar.apps.rpc.JSONRPC` handler which exposes six functions
for executing tasks and retrieving task information.

The :ref:`task-queue example <tutorials-taskqueue>` shows how to use this class
in the context of a WSGI server running along side the task-queue application.

TaskQueue Rpc Mixin
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskQueueRpcMixin
   :members:
   :member-order: bysource
'''
import pulsar
from pulsar.apps import rpc

from .backends import Task, TaskNotAvailable


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
and a :ref:`task queue <apps-taskqueue>` application installed in the
:class:`pulsar.Arbiter`.

:parameter taskqueue: instance or name of the
    :class:`pulsar.apps.tasks.TaskQueue` which exposes the remote procedure
    calls.
    
'''
    _task_backend = None
    def __init__(self, taskqueue, **kwargs):
        if not isinstance(taskqueue, str):
            taskqueue = taskqueue.name
        self.taskqueue = taskqueue
        super(TaskQueueRpcMixin,self).__init__(**kwargs)
        
    ############################################################################
    ##    REMOTES
    def rpc_job_list(self, request, jobnames=None):
        '''Return the list of Jobs registered with task queue with meta
information. If a list of jobnames is given, it returns only jobs
included in the list.'''
        task_backend = yield self.task_backend()
        yield task_backend.job_list(jobnames=jobnames)
    
    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self._rq(request, 'next_scheduled', jobnames=jobnames)
        
    def rpc_run_new_task(self, request, jobname=None, **kw):
        '''Run a new task in the task queue. The task can be of any type
as long as it is registered in the task queue registry. To check the
available tasks call the "job_list" function. It returns the task id.'''
        result = yield self.run_new_task(request, jobname, **kw)
        yield task_to_json(result)
        
    def rpc_get_task(self, request, id=None):
        '''Retrieve a task from its id'''
        if id:
            task_backend = yield self.task_backend()
            result = yield task_backend.get_task(id)
            yield task_to_json(result)
    
    def rpc_get_tasks(self, request, **filters):
        '''Retrieve a list of tasks which satisfy key-valued filters'''
        if filters:
            task_backend = yield self.task_backend()
            result = yield task_backend.get_tasks(**filters)
            yield task_to_json(result)
        
    def rpc_wait_for_task(self, request, id=None, timeout=None):
        '''Wait for a task to have finished.
        
        :param id: the id of the task to wait for.
        :param timeout: optional timeout in seconds.
        :return: the json representation of the task once it has finished.
        '''
        if id:
            task_backend = yield self.task_backend()
            result = yield task_backend.wait_for_task(id, timeout=timeout)
            yield task_to_json(result)
            
    def rpc_num_tasks(self, request):
        '''Return the approximate number of tasks in the task queue.'''
        task_backend = yield self.task_backend()
        yield task_backend.num_tasks()
    
    ############################################################################
    ##    INTERNALS
    def task_backend(self):
        if not self._task_backend:
            app = yield pulsar.get_application(self.taskqueue)
            self._task_backend = app.backend
        yield self._task_backend
        
    def run_new_task(self, request, jobname, args=None, meta_data=None, **kw):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        meta_data = meta_data or {}
        meta_data.update(self.task_request_parameters(request))
        args = args or ()
        task_backend = yield self.task_backend()
        yield task_backend.run_job(jobname, args, kw, **meta_data)
    
    def task_request_parameters(self, request):
        '''**Internal function** which returns a dictionary of parameters
to be passed to the :class:`Task` class constructor.
This function can be overridden to add information about
the type of request, who made the request and so forth. It must return
a dictionary. By default it returns an empty dictionary.'''
        return {}
    
    def _rq(self, request, action, *args, **kw):
        return pulsar.send(self.taskqueue, action, *args, **kw)
    
