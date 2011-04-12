import pulsar
import pulsar.workers.sync as sync


class Worker(sync.WsgiSyncMixin,pulsar.WorkerThreadPool):
    '''An asyncronous worker based on a thread pool.'''            
    pass