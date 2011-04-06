import pulsar
import pulsar.workers.sync as sync


class Worker(sync.WsgiSyncMixin,pulsar.WorkerThread):
    '''A syncronous worker on a thread.'''
    
    def reset_socket(self):
        self.socket.setblocking(0)