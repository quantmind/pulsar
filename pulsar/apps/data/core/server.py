'''Classes for the pulsar Key-Value store server
'''
import re
import time
import math
from functools import partial
from collections import namedtuple
from marshal import dump, load

import pulsar
from pulsar.apps.socket import SocketServer
from pulsar.utils.config import Global

from .parser import redis_parser


class RedisParserSetting(Global):
    name = "py_redis_parser"
    flags = ["--py-redis-parser"]
    action="store_true"
    default=False
    desc='Use the python redis parser rather the C++ implementation.'

    def parser_class(self):
        return PyRedisParser if self.value else RedisParser


class KeyValuePairSetting(pulsar.Setting):
    virtual = True
    app = 'keyvaluestore'
    section = "Key-value pair server"


class KeyValueDatabases(KeyValuePairSetting):
    name = "key_value_databases"
    flags = ["--key-value-databases"]
    type=int
    default=16
    desc='Number of databases for the key value store.'


class KeyValuePassword(KeyValuePairSetting):
    name = "key_value_password"
    flags = ["--key-value-password"]
    default=''
    desc='Optional password for the database.'


class KeyValueFileName(KeyValuePairSetting):
    name = "key_value_filename"
    flags = ["--key-value-filename"]
    default='pulsarkv.rdb'
    desc='The filename where to dump the DB.'


class CommandError(Exception):
    error_type = 'ERR'

    def __init__(self, message, error_type=None):
        super(CommandError, self).__init__(msg)
        if error_type:
            self.error_type = error_type


class PulsarStoreConnection(pulsar.Connection):
    '''Used both by client and server'''
    def __init__(self, *args, **kw):
        super(PulsarStoreConnection, self).__init__(*args, **kw)
        self.parser = self._producer._parser_class()
        self.database = 0
        self.password = ''


class PulsarStoreProtocol(pulsar.ProtocolConsumer):

    def data_received(self, data):
        conn = self._connection
        server = conn._producer
        parser = conn.parser
        store = server._key_value_store.databases[conn.database]
        parser.feed(data)
        request = parser.get()
        if request is not False:
            command = request[0].decode('utf-8').lower()
            name = COMMAND_NAMES.get(command, command)
            try:
                handle = getattr(store, name, None)
                if not handle:
                    raise CommandError("unkwnown command '%s'" % command)
                if server.password != conn.password:
                    if command != 'auth':
                        raise CommandError("not authenticated")
                handle(conn, request)
            except CommandError as e:
                msg = '-%s %s\r\n' % (e.error_type, e)
                conn._transport.write(msg.encode('utf-8'))
                self.finished()
            except Exception as e:
                msg = '-SERVERERR %s\r\n' % e
                conn._transport.write(msg.encode('utf-8'))
                raise
            else:
                self.finished()


class TcpServer(pulsar.TcpServer):

    def __init__(self, cfg, *args, **kwargs):
        super(TcpServer, self).__init__(*args, **kwargs)
        self.cfg = cfg
        self._parser_class = redis_parser(cfg.py_redis_parser)
        self._key_value_store = Storage(self, cfg.key_value_databases)
        self.password = cfg.key_value_password

    def info(self):
        info = super(TcpServer, self).info()
        info.update(self._key_value_store._info())
        return info


class KeyValueStore(SocketServer):
    '''A :class:`.SocketServer` which serve a key-value store similar to redis.
    '''
    name = 'keyvaluestore'
    cfg = pulsar.Config(bind='127.0.0.1:6410',
                        keep_alive=0,
                        apps=['socket', 'keyvaluestore'])

    def server_factory(self, *args, **kw):
        return TcpServer(self.cfg, *args, **kw)

    def protocol_factory(self):
        consumer_factory = partial(PulsarStoreProtocol, self.cfg)
        return partial(PulsarStoreConnection, consumer_factory)

    def monitor_start(self, monitor):
        cfg = self.cfg
        workers = min(1, cfg.workers)
        cfg.set('workers', workers)
        return super(KeyValueStore, self).monitor_start(monitor)


###############################################################################
##    DATA STORE
pubsub_patterns = namedtuple('pubsub_patterns', 're clients')
COMMAND_NAMES = {}


class command_name:

    def __init__(self, name):
        self.name = name

    def __call__(self, f):
        COMMAND_NAMES[self.name] = f.__name__
        return f


def check_input(command, N, failed, msg):
    if failed:
        raise CommandError("command '%s' %s, %s given" % (command, msg, N))


class Db(object):
    '''The database.
    '''
    def __init__(self, num, storage):
        self._num = num
        self._storage = storage
        self._parser = storage._parser
        self._data = {}
        self._timeouts = {}

    def __repr__(self):
        return 'db%s' % self._num
    __str__ = __repr__

    ###########################################################################
    ##    KEYS COMMANDS
    @command_name('del')
    def delete(self, connection, request):
        N = len(request) - 1
        check_input('del', N, not N, 'expects 1 or more arguments')
        pop = self._pop
        N = reduce(lambda x, y: x+y, (pop(key) for key in requests[1:]))
        connection._transport.write((':%d\r\n' % N).encode('utf-8'))

    def exists(self, connection, request):
        N = len(request) - 1
        check_input('exists', N, N != 1, 'expects 1 argument')
        value = bool(request[1] in self._data)
        connection._transport.write((':%d\r\n' % value).encode('utf-8'))

    ###########################################################################
    ##    STRING COMMANDS
    def get(self, connection, request):
        N = len(request) - 1
        if N != 1:
            raise CommandError('get expects 1 argument, %s given' % N)
        value = self._get(request[1])
        connection._transport.write(self._parser.bulk_replay(value))

    def mget(self, connection, request):
        N = len(request) - 1
        if not N:
            raise CommandError("'mget' expects 1 or more arguments, %s given"
                               % N)
        get = self._get
        values = tuple((get(k) for k in request[1:]))
        connection._transport.write(self._parser.pack_command(*values))

    def set(self, connection, request):
        N = len(request) - 1
        if N < 2 or N > 3:
            raise CommandError('set expects 2 or 3 arguments, %s given' % N)
        key, value = request[1], request[2]
        if N == 3:
            timeout = float(request[3])
            self._expire(key, timeout)
            self._data[key] = value
        else:
            if key in self._timeouts:
                self._timeouts.pop(key).cancel()
            self._data[key] = value
        connection._transport.write(b'+OK\r\n')

    ###########################################################################
    ##    PUBSUB COMMANDS
    def psubscribe(self, connection, request):
        N = len(request) - 1
        check_input('psubscribe', N, not N, 'expects one argument')
        self._storage._psubscribe(connection, request[1:])

    def pubsub(self, connection, request):
        N = len(request) - 1
        check_input('pubsub', N, not N, 'expects at least 1 argument')
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'channels':
            if N == 2:
                pattern = request[2]
            elif N > 2:
                check_input('pubsub channels', N, False,
                            'expects at least max two arguments')
            else:
                pattern = None
            self._storage._pubsub_channels(connection, pattern)
        elif subcommand == 'numsub':
            self._storage._pubsub_numsub(connection, request[2:])
        elif subcommand == 'numpat':
            check_input('pubsub numpat', N, N > 1, 'expects no arguments')
            self._storage._pubsub_numpat(connection, request[2:])
        else:
            raise CommandError("unknown command 'pubsub %s'" % subcommand)

    def publish(self, connection, request):
        N = len(request) - 1
        if N != 2:
            raise CommandError(('command "publish" expects two arguments, '
                                '%s given' % N))
        self._storage._publish(connection, request[1], request[2])

    def punsubscribe(self, connection, request):
        N = len(request) - 1
        if not N:
            raise CommandError(('command "punsubscribe" expects 1 argument, '
                                '%s given' % N))
        self._storage._punsubscribe(connection, request[1:])

    def subscribe(self, connection, request):
        N = len(request) - 1
        if not N:
            raise CommandError(('command "subscribe" expects 1 argument, '
                                '%s given' % N))
        self._storage._subscribe(connection, request[1:])

    def unsubscribe(self, connection, request):
        N = len(request) - 1
        if not N:
            raise CommandError(('command "unsubscribe" expects 1 argument, '
                                '%s given' % N))
        self._storage._unsubscribe(connection, request[1:])

    ###########################################################################
    ##    CONNECTION COMMANDS
    def auth(self, connection, request):
        N = len(request) - 1
        if N != 1:
            raise CommandError(('command "auth" expects 1 argument, '
                                '%s given' % N))
        conn.password = request[1].encode('utf-8')
        if conn.password != conn._producer.password:
            raise CommandError("wrong password")
        connection._transport.write(b'+OK\r\n')

    def echo(self, connection, request):
        N = len(request) - 1
        if N != 1:
            raise CommandError(('command "echo" expects 1 argument, '
                                '%s given' % N))
        chunk = self._parser.bulk_replay(request[1])
        connection._transport.write(chunk)

    def ping(self, connection, request):
        if len(request) != 1:
            raise CommandError('ping expect no arguments')
        connection._transport.write(b'+PONG\r\n')

    def quit(self, connection, request):
        if len(request) != 1:
            raise CommandError("'quit' expect no arguments")
        connection._transport.write(b'+OK\r\n')
        connection.close()

    def select(self, connection, request):
        N = len(request) - 1
        if N != 1:
            raise CommandError('select expect 1 argument, %s given', N)
        D = len(self._storage.databases) - 1
        try:
            num = int(request[1])
            if num < 0 or num > D:
                raise ValueError
        except Exception:
            raise CommandError(
                'select requires a database number between %s and %s' % (0, D))
        connection.database = num
        connection._transport.write(b'+OK\r\n')

    ###########################################################################
    ##    SERVER COMMANDS
    def client(self, connection, request):
        if len(request) < 2:
            raise CommandError("'client' expects at least 1 argument")
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'list':
            if len(request) > 2:
                raise CommandError("'client list' no arguments")
            value = self._parser.pack_command(tuple(self._client_list))
        else:
            raise CommandError("unknown command 'client %s'" % subcommand)
        connection._transport.write(value)

    def dbsize(self, connection, request):
        if len(request) != 1:
            raise CommandError("'dbsize' expects no arguments")
        N = len(self._data)
        connection._transport.write((':%d\r\n' % N).encode('utf-8'))

    def flushdb(self, connection, request):
        if len(request) != 1:
            raise CommandError("'flushdb' expects no arguments")
        self._flush()
        connection._transport.write(b'+OK\r\n')

    def flushall(self, connection, request):
        if len(request) != 1:
            raise CommandError("'flushall' expects no arguments")
        for db in self._storage.databases.values():
            db._flush()
        connection._transport.write(b'+OK\r\n')

    def info(self, connection, request):
        if len(request) != 1:
            raise CommandError('info expect no arguments')
        info = '\n'.join(self._flat_info())
        chunk = self._parser.bulk_replay(info.encode('utf-8'))
        connection._transport.write(chunk)

    def time(self, connection, request):
        if len(request) != 1:
            raise CommandError("'time' expect no arguments")
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        info = '\n'.join(self._flat_info())
        chunk = self._parser.pack_command(seconds, microseconds)
        connection._transport.write(chunk)

    ###########################################################################
    ##    INTERNALS
    def _get(self, key, default=None):
        if key in self._data:
            self._storage._hit_keys += 1
            return self._data[key]
        else:
            self._storage._missed_keys += 1
            return default

    def _pop(self, key):
        if key in self._data:
            self._storage._hit_keys += 1
            self._data.pop(key)
            return 1
        else:
            self._storage._missed_keys += 1
            return 0

    def _flush(self):
        timeouts = list(self._timeouts.values())
        self._data.clear()
        self._timeouts.clear()
        for timeout in timeouts:
            timeout.cancel()

    def _flat_info(self):
        info = self._storage._server.info()
        e = self._encode_info_value
        for k, values in info.items():
            if isinstance(values, dict):
                yield '#%s' % k
                for key, value in values.items():
                    if isinstance(value, (list, tuple)):
                        value = ', '.join((e(v) for v in value))
                    elif isinstance(value, dict):
                        value = ', '.join(('%s=%s' % (k, e(v))
                                           for k, v in value.items()))
                    else:
                        value = e(value)
                    yield '%s:%s' % (key, value)

    def _info(self):
        return {'keys': len(self._data),
                'expires': len(self._timeouts)}

    def _encode_info_value(self, value):
        return str(value).replace('=', ' ').replace(',', ' ')


class Storage(object):

    def __init__(self, server, databases, password=None):
        self._password = password
        self._server = server
        self._loop = server._loop
        self._parser = server._parser_class()
        self._missed_keys = 0
        self._hit_keys = 0
        self._channels = {}
        self._patterns = {}
        self.databases = dict(((num, Db(num, self))
                               for num in range(databases)))

    def _info(self):
        keyspace = {}
        stats = {'keyspace_hits': self._hit_keys,
                 'keyspace_misses': self._missed_keys}
        for db in self.databases.values():
            if db._data:
                keyspace[str(db)] = db._info()
        return {'keyspace': keyspace, 'stats': stats}

    def _save(self):
        dbs = [db for db in self.databases.values() if db.size]
        with open(self._server.cfg.key_value_filename, 'w') as file:
            dump(dbs, file)

    ##    PUBSUB
    def _pubsub_channels(self, connection, pattern):
        if pattern:
            pre = re.compile(pattern.decode('utf-8', errors='ignore'))
            channels = []
            for channel in self._channels:
                if pre.match(channel.decode('utf-8', errors='ignore')):
                    channels.append(channel)
        else:
            channels = list(self._channels.values())
        chunk = self._parser.pack_command(*channels)
        connection._transport.write(chunk)

    def _pubsub_numsub(self, connection, channels):
        count = 0
        for channel in channels:
            clients = self._channels.get(channel, ())
            count += len(clients)
        connection._transport.write((':%d\r\n' % count).encode('utf-8'))

    def _pubsub_numpat(self, connection):
        count = reduce(lambda x, y: x + len(y.clients),
                       self._patterns.values())
        connection._transport.write((':%d\r\n' % count).encode('utf-8'))

    def _publish(self, connection, channel, message):
        ch = channel.decode('utf-8')
        clients = self._channels.get(channel)
        msg = self._parser.pack_command(b'message', channel, message)
        count = self.__publish_clients(msg, self._channels.get(channel, ()))
        for pattern in self._patterns.values():
            g = pattern.re.match(ch)
            if g:
                count += self.__publish_clients(msg, pattern.clients)
        connection._transport.write((':%d\r\n' % count).encode('utf-8'))

    def _psubscribe(self, connection, patterns):
        for pattern in patterns:
            p = self._patterns.get(pattern)
            if not p:
                p = pubsub_patterns(re.compile(pat.decode('utf-8')), set())
                self._patterns[pattern] = p
            p.clients.add(connection)
            count = reduce(lambda x, y: x + int(connection in y.clients),
                           self._patterns.values())
            chunk = self._parser.packcommand(b'psubscribe', pattern, count)
            connection._transport.write(chunk)

    def _punsubscribe(self, connection, patterns):
        for pattern in patterns:
            pattern = pattren.decode('utf-8')
            clients = self._patterns.get(pattern)
            if not clients:
                clients = set()
                self._patterns[pattern] = clients
            clients.add(connection)

    def _subscribe(self, connection, channels):
        for channel in channels:
            clients = self._channels.get(channel)
            if not clients:
                clients = set()
                self._channels[channel] = clients
            clients.add(connection)

    def _unsubscribe(self, connection, channels):
        channels = channels or self._channels
        for channel in channels:
            clients = self._channels.get(channel)
            if clients:
                clients.discard(connection)

    def __publish_clients(self, msg, clients):
        remove = set()
        count = 0
        for client in clients:
            try:
                client._transport.write(msg)
                count += 1
            except Exception:
                remove.append(client)
        if remove:
            clients.difference_update(remove)
        return count
