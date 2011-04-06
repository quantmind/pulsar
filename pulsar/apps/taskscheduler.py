import pulsar

class NumTaskWorkers(pulsar.Setting):
    name = "taskworkers"
    section = "Worker Processes"
    cli = ['--taskworkers']
    meta = "INT"
    validator = pulsar.validate_pos_int
    type = "int"
    default = 1
    desc = """\
        The number of worker process for handling tasks requests.
        
        A positive integer generally in the 2-4 x $(NUM_CORES) range. You'll
        want to vary this a bit to find the best for your particular
        application's work load.
    """

class TaskScheduler(pulsar.Application):
    