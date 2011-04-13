import pulsar

class TaskQueue(pulsar.Setting):
    name = "taskqueue"
    section = "Worker Processes"
    cli = ['--taskqueue']
    meta = "INT"
    validator = pulsar.validate_pos_int
    type = "int"
    default = 1
    desc = """\
The task-queue class used.

1) python for python based task queue
2) redis for Redis based task queue    
    """

class TaskScheduler(pulsar.Application):
    '''A task scheduler with a JSON-RPC hook for remote procedure calls'''
    