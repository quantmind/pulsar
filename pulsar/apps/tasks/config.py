import pulsar
from pulsar import to_string


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
