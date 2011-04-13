from pulsar.utils.importer import import_modules

ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


class TaskConsumer(object):
    
    def __init__(self, cfg):
        self.cfg = cfg
        import_modules(cfg.tasks_path)
    
    def __call__(self, req):
        pass
        