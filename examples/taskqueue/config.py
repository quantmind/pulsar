
task_worker_class = 'task_t'
task_workers = 2

worker_class = 'httpt_t'
num_workers = 5


def rpc_handler(wp):
    from taskqueue.impl import Handler
    h = Handler()
    return h
