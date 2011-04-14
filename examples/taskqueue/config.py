
worker_class = 'http_t'
num_workers = 5

task_worker_class = 'task_t'
task_workers = 2


def rpc_handler(wp):
    from taskqueue.impl import Handler
    h = Handler()
    return h
