import pulsar
from pulsar.utils.py2py3 import *


def validate_list(val):
    if isinstance(val,list):
        return val
    elif isinstance(val,tuple):
        return list(val)
    else:
        val = to_string(val).split(',')
        vals = []
        for v in to_string(val).split(','):
            v = v.strip()
            if v:
                vals.append(v)
        return vals


class TaskPath(pulsar.Setting):
    name = "tasks_path"
    section = "Task Consumer"
    meta = "STRING"
    validator = validate_list
    cli = ["--tasks-path"]
    default = ['pulsar.apps.tasks.testing']
    desc = """\
        List of python dotted paths where tasks are located.
        """


class TaskWorker(pulsar.Setting):
    name = "task_worker_class"
    section = "Task Consumer"
    meta = "STRING"
    cli = ["--task-worker-class"]
    validator = pulsar.validate_string
    default = 'task'
    desc = """\
        The type of workers to use for consuming tasks. It is a string
        A string referring to one of the following bundled classes:
        
        * ``task`` (Default) - Task consumer on a Process
        * ``task_t`` - Task consumer on a Thread
        """


class TaskWorkers(pulsar.Setting):
    name = "task_workers"
    section = "Task Consumer"
    cli = ["--task-workers"]
    meta = "INT"
    validator = pulsar.validate_pos_int
    type = "int"
    default = 1
    desc = """\
        The number of task worker process for handling tasks.
        
        A positive integer generally in the 2-4 x $(NUM_CORES) range. You'll
        want to vary this a bit to find the best for your particular
        application's work load.
        """


def default_rpc(wp):
    pass


class RpcHandler(pulsar.Setting):
    name = "rpc_handler"
    section = "Task Consumer"
    validator = pulsar.validate_callable(1)
    default = staticmethod(default_rpc)
    type = "callable"
    desc = """\
        Setup the Rpc handler.
        """
