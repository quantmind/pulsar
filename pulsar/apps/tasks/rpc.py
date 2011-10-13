import pulsar
from pulsar.apps import rpc


__all__ = ['TaskQueueRpcMixin']


def task_to_json(task):
    if task:
        return task.tojson()


class TaskQueueRpcMixin(rpc.JSONRPC):
    '''A mixin RPC class for communicating with a task queue.
To use this mixin, you need to have an :ref:`RPC application <apps-rpc>`
and a :ref:`task queue <apps-tasks>` application installed in the arbiter.

.. attribute:: task_queue_manager

    A :class:`pulsar.ActorLink` for facilitating the communication
    from the rpc workers to the task queue.
    
It exposes the following functions:

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
    
    task_queue_manager = pulsar.ActorLink('taskqueue')
    
    def task_request_parameters(self, request):
        '''return a dictionary of parameters to be passed to the :class:`Task`
class during construction. This function can be used to add information about
the type of request, who made the request and so forth.'''
        return {}
    
    def task_callback(self, request, jobname, ack = True, **kwargs):
        '''Return a callable for running a task from *jobname* in
the tsak queue.'''
        funcname = 'addtask' if ack else 'addtask_noack'
        request_params = self.task_request_parameters(request)
        return self.task_queue_manager.get_callback(
                                            request.environ,
                                            funcname,
                                            jobname = jobname,
                                            task_extra = request_params,
                                            **kwargs)    
        
    def rpc_job_list(self, request, jobnames = None):
        '''Dictionary of information about the registered jobs. If
*jobname* is passed, information regrading the specific job will be returned.'''
        return self.task_queue_manager(request.environ,
                                       'job_list',
                                       jobnames = jobnames)
    
    def rpc_next_scheduled_task(self, request, jobname = None):
        '''Return a two elements tuple containing the job name of the next scheduled task
and the time in seconds when the task will run.

:parameter jobname: optional jobname.'''
        return self.task_queue_manager(request.environ,
                                       'next_scheduled',
                                       jobname = jobname)
        
    def rpc_run_new_task(self, request, jobname = None, ack = True, **kwargs):
        '''Run a new task in the task queue. The task can be of any type
as long as it is registered in the job registry.

:parameter jobname: the name of the job to run.
:parameter ack: if ``True`` the request will be send to the caller.
:parameter kwargs: optional task parameters.'''
        result = self.task_callback(request, jobname, ack, **kwargs)()
        return result.add_callback(task_to_json)
        
    def rpc_get_task(self, request, id = None):
        if id:
            return self.task_queue_manager(request.actor,
                                           'get_task',
                                           id).add_callback(task_to_json)
                           
    
        
    