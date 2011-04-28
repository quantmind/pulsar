from multiprocessing import Queue

from pulsar import mq


class Channel(mq.Channel):
    queues = {}
    do_restore = False

    def _new_queue(self, queue, **kwargs):
        if queue not in self.queues:
            self.queues[queue] = Queue()

    def _get(self, queue, timeout=None):
        return self.queues[queue].get(block=False)

    def _put(self, queue, message, **kwargs):
        self.queues[queue].put(message)

    def _size(self, queue):
        return self.queues[queue].qsize()

    def _delete(self, queue):
        self.queues.pop(queue, None)

    def _purge(self, queue):
        q = self.queues[queue]
        size = q.qsize()
        self.queues[queue].queue.clear()
        return size


class Transport(mq.Transport):
    Channel = Channel

    #: memory backend state is global.
    state = virtual.BrokerState()

    def establish_connection(self):
        pass