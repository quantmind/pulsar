
import pulsar.workers.base as base
import pulsar.workers.sync as sync


class Worker(sync.SyncMixin,base.WorkerThread):
    pass