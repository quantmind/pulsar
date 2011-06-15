from pulsar.http import actor_call, rpc
from .utils import SendToQueue

__all__ = ['TaskQueueRpcMixin']


class TaskQueueRpcMixin(rpc.JSONRPC):
    '''A mixin RPC class for communicationg with a task queue'''
    
    task_queue_manager = 'taskqueue'
    
    def rpc_job_list(self, request):
        '''Dictionary of information about the registered jobs.'''
        return actor_call(request,
                          self.task_queue_manager,
                          'job_list')
    
    def rpc_next_scheduled_task(self, request, jobname = None):
        return actor_call(request,
                          self.task_queue_manager,
                          'next_scheduled',
                          jobname = 'liverefresh')
        
    def rpc_run_new_task(self, request, jobname = None, ack = True, **kwargs):
        return SendToQueue(jobname,
                           request,
                           self.task_queue_manager,
                           ack)(**kwargs)
        
    