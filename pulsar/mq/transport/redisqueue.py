from pulsar import mq

class Channel(mq.Channel):

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()

    def _get_client(self):
        from redis import Redis
        return Redis

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    def drain_events(self, timeout=None):
        return self._poller.poll()

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        self.client.sadd(self.keyprefix_queue % (exchange, ),
                         self.sep.join([routing_key or "",
                                        pattern or "",
                                        queue or ""]))

    def get_table(self, exchange):
        members = self.client.smembers(self.keyprefix_queue % (exchange, ))
        return [tuple(val.split(self.sep)) for val in members]

    def _get(self, queue):
        item = self.client.rpop(queue)
        if item:
            return deserialize(item)
        raise Empty()

    def _size(self, queue):
        return self.client.llen(queue)

    def _get_many(self, queues, timeout=None):
        dest__item = self.client.brpop(queues, timeout=timeout)
        if dest__item:
            dest, item = dest__item
            return deserialize(item), dest
        raise Empty()

    def _put(self, queue, message, **kwargs):
        self.client.lpush(queue, serialize(message))

    def _purge(self, queue):
        size = self.client.llen(queue)
        self.client.delete(queue)
        return size

    def close(self):
        self._poller.close()
        if self._client is not None:
            try:
                self._client.bgsave()
            except self.ResponseError:
                pass
            try:
                self._client.connection.disconnect()
            except (AttributeError, self.ResponseError):
                pass
        super(Channel, self).close()

    def _open(self):
        conninfo = self.connection.client
        database = conninfo.virtual_host
        if not isinstance(database, int):
            if not database or database == "/":
                database = DEFAULT_DB
            elif database.startswith("/"):
                database = database[1:]
            try:
                database = int(database)
            except ValueError:
                raise ValueError(
                    "Database name must be int between 0 and limit - 1")

        return self.Client(host=conninfo.hostname,
                           port=conninfo.port or DEFAULT_PORT,
                           db=database,
                           password=conninfo.password)

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(mq.Transport):
    Channel = Channel

    interval = 1

