'''Classes for the pulsar Key-Value store server
'''
import re
import time
import math
from functools import partial, reduce
from collections import namedtuple
from marshal import dump, load

import pulsar
from pulsar.apps.socket import SocketServer
from pulsar.utils.config import Global
from pulsar.utils.structures import Hash


from .parser import redis_parser


DEFAULT_PULSAR_STORE_ADDRESS = '127.0.0.1:6410'

# Keyspace changes notification classes
REDIS_NOTIFY_KEYSPACE = (1<<0)
REDIS_NOTIFY_KEYEVENT = (1<<1)
REDIS_NOTIFY_GENERIC = (1<<2)
REDIS_NOTIFY_STRING = (1<<3)
REDIS_NOTIFY_LIST = (1<<4)
REDIS_NOTIFY_SET = (1<<5)
REDIS_NOTIFY_HASH = (1<<6)
REDIS_NOTIFY_ZSET = (1<<7)
REDIS_NOTIFY_EXPIRED = (1<<8)
REDIS_NOTIFY_EVICTED = (1<<9)
REDIS_NOTIFY_ALL = (REDIS_NOTIFY_GENERIC | REDIS_NOTIFY_STRING |
                    REDIS_NOTIFY_LIST | REDIS_NOTIFY_SET |
                    REDIS_NOTIFY_HASH | REDIS_NOTIFY_ZSET |
                    REDIS_NOTIFY_EXPIRED | REDIS_NOTIFY_EVICTED)

REDIS_MULTI  = (1 << 3)
REDIS_BLOCKED = (1 << 4)


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
    message = None

    def __init__(self, message=None, error_type=None):
        super(CommandError, self).__init__(message or self.message)
        if error_type:
            self.error_type = error_type


class WrongType(CommandError):
    error_type = 'WRONGTYPE'
    message = 'Operation against a key holding the wrong kind of value'


class PulsarStoreConnection(pulsar.Connection):
    '''Used both by client and server'''
    bpop = None
    flag = 0
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
    cfg = pulsar.Config(bind=DEFAULT_PULSAR_STORE_ADDRESS,
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
        self._expires = {}
        self._key_space_events = {}
        self._live_handles = set()
        self.hashtype = Hash
        self.list_type = list

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
    def append(self, connection, request):
        N = len(request) - 1
        check_input('append', N, N != 2, 'expects two arguments')
        key = request[1]
        value = self._get(key)
        if value is None:
            value = request[2]
            self._data[key] = value
        elif not isinstance(value, bytes):
            raise WrongType
        else:
            value = value + request[2]
            self._data[key] = value
        result = len(value)
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def get(self, connection, request):
        N = len(request) - 1
        if N != 1:
            raise CommandError('get expects 1 argument, %s given' % N)
        value = self._get(request[1])
        connection._transport.write(self._parser.bulk(value))

    def mget(self, connection, request):
        N = len(request) - 1
        if not N:
            raise CommandError("'mget' expects 1 or more arguments, %s given"
                               % N)
        get = self._get
        values = tuple((get(k) for k in request[1:]))
        connection._transport.write(self._parser.multi_bulk(*values))

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
            if key in self._expires:
                self._expires.pop(key).cancel()
            self._data[key] = value
        connection._transport.write(b'+OK\r\n')

    ###########################################################################
    ##    HASHES COMMANDS
    def hdel(self, connection, request):
        N = len(request) - 1
        check_input('hdel', N, N < 2, 'expects at least two arguments')
        value = self._get(request[1])
        if value is None:
            rem = 0
        elif getattr(value, 'datatype', 'hash'):
            rem = value.delete(request[2:])
            if not value:
                self._data.pop(request[1])
        else:
            raise WrongType
        connection._transport.write((':%d\r\n' % rem).encode('utf-8'))

    def hexists(self, connection, request):
        N = len(request) - 1
        check_input('hexists', N, N != 2, 'expects at two arguments')
        value = self._get(request[1])
        if value is None:
            rem = 0
        elif getattr(value, 'datatype', 'hash'):
            rem = int(request[2] in value)
        else:
            raise WrongType
        connection._transport.write((':%d\r\n' % rem).encode('utf-8'))

    def hget(self, connection, request):
        N = len(request) - 1
        check_input('hget', N, N != 2, 'expects at two arguments')
        value = self._get(request[1])
        if value is None:
            result = None
        elif getattr(value, 'datatype', 'hash'):
            result = value.get(request[2])
        else:
            raise WrongType
        connection._transport.write(self._parser.bulk(result))

    def hgetall(self, connection, request):
        N = len(request) - 1
        check_input('hgetall', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = value.flat()
        else:
            raise WrongType
        connection._transport.write(self._parser.multi_bulk(result))

    def hincrby(self, connection, request):
        result = self._hincrby('hincrby', request, int)
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def hincrbyfloat(self, connection, request):
        result = self._hincrby('hincrbyfloat', request, float)
        result = str(result).encode('utf-8')
        connection._transport.write(self._parser.bulk(result))

    def hkeys(self, connection, request):
        N = len(request) - 1
        check_input('hkeys', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = tuple(value)
        else:
            raise WrongType
        connection._transport.write(self._parser.multi_bulk(result))

    def hlen(self, connection, request):
        N = len(request) - 1
        check_input('hlen', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = 0
        elif getattr(value, 'datatype', 'hash'):
            result = len(value)
        else:
            raise WrongType
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def hmget(self, connection, request):
        N = len(request) - 1
        check_input('hget', N, N < 3, 'expects at least three arguments')
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = value.mget(request[2:])
        else:
            raise WrongType
        connection._transport.write(self._parser.multi_bulk(result))

    def hmset(self, connection, request):
        N = len(request) - 1
        check_input('hget', N, N < 3 or (N-1) // 2 * 2 == N-1,
                    'expects at least three arguments')
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        value.update(zip(request[2::2], request[3::2]))
        connection._transport.write(b'+OK\r\n')

    def hset(self, connection, request):
        N = len(request) - 1
        check_input('hget', N, N != 3, 'expects three arguments')
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        result = int(request[3] in value)
        value[request[2]] = request[3]
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def hsetnx(self, connection, request):
        N = len(request) - 1
        check_input('hget', N, N != 3, 'expects three arguments')
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        result = int(request[3] in value)
        if not result:
            value[request[2]] = request[3]
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def hvals(self, connection, request):
        N = len(request) - 1
        check_input('hvals', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = tuple(value.values())
        else:
            raise WrongType
        connection._transport.write(self._parser.multi_bulk(result))

    ###########################################################################
    ##    LIST COMMANDS
    def blpop(self, connection, request):
        N = len(request) - 1
        check_input('blpop', N, N < 2, 'expects at least two arguments')
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._lpop(connection, keys, 'pop_left'):
            self.wait_for_keys(timeout, keys, self._lpop, connection, keys,
                               'pop_left')

    def brpop(self, connection, request):
        N = len(request) - 1
        check_input('blpop', N, N < 2, 'expects at least two arguments')
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._lpop(connection, keys, 'pop'):
            self.wait_for_keys(timeout, keys, self._lpop, connection, keys,
                               'pop')

    def lindex(self, connection, request):
        N = len(request) - 1
        check_input('blpop', N, N != 2, 'expects two arguments')
        value = self._get(request[1])
        if value is None:
            result = None
        elif isinstance(value, self.list_type):
            index = int(request[2])
            if index >= 0 and index < len(value):
                result = value[index]
            else:
                result = None
        else:
            raise WrongType
        connection._transport.write(self._parser.bulk(result))

    def llen(self, connection, request):
        N = len(request) - 1
        check_input('blpop', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = 0
        elif isinstance(value, self.list_type):
            result = len(value)
        else:
            raise WrongType
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def lpush(self, connection, request):
        self._list_push(connection, request, 'extend_left')

    def rpush(self, connection, request):
        self._list_push(connection, request, 'extend_left')

    def _lpop(self, connection, keys, method):
        list_type = self.list_type
        for key in keys:
            value = self._get(key)
            if isinstance(value, list_type):
                result = (key, geattr(value, mehod)())
                self._remove_value(key, value)
                connection._transport.write(self._parser.multi_bulk(result))
                return True
            elif value is not None:
                raise WrongType

    ###########################################################################
    ##    SETS COMMANDS
    def sadd(self, connection, request):
        N = len(request) - 1
        check_input('sadd', N, N < 2, 'expects at least two arguments')
        value = self._get(request[1])
        if value is None:
            value = set()
            self._data[request[1]] = value
        elif not isinstance(value, set):
            raise WrongType
        data = request[2:]
        result = reduce(lambda x, y: x + int(y in value), data)
        value.update(data)
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def scard(self, connection, request):
        N = len(request) - 1
        check_input('sadd', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = 0
        elif not isinstance(value, set):
            raise WrongType
        else:
            result = len(value)
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def sdiff(self, connection, request):
        N = len(request) - 1
        check_input('sdiff', N, N < 1, 'expects at least one argument')
        self._setoper(connection, 'difference', request[1:])

    def sdiffstore(self, connection, request):
        N = len(request) - 1
        check_input('sdiffstore', N, N < 2, 'expects at least two argument')
        self._setoper(connection, 'difference', request[2:], request[1])

    def sinter(self, connection, request):
        N = len(request) - 1
        check_input('sinter', N, N < 1, 'expects at least one argument')
        self._setoper(connection, 'intersection', request[1:])

    def sinterstore(self, connection, request):
        N = len(request) - 1
        check_input('sinterstore', N, N < 2, 'expects at least two argument')
        self._setoper(connection, 'intersection', request[2:], request[1])

    def sismemeber(self, connection, request):
        N = len(request) - 1
        check_input('sismemeber', N, N != 2, 'expects two arguments')
        value = self._get(request[1])
        if value is None:
            result = 0
        elif not isinstance(value, set):
            raise WrongType
        else:
            result = int(request[2] in value)
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def smembers(self, connection, request):
        N = len(request) - 1
        check_input('sismemeber', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            value = ()
        elif not isinstance(value, set):
            raise WrongType
        connection._transport.write(self._parser.multi_bulk(value))

    def smove(self, connection, request):
        N = len(request) - 1
        check_input('smove', N, N != 3, 'expects three argument')
        orig = self._get(request[1])
        dest = self._get(request[2])
        if orig is None:
            result = 0
        elif not isinstance(orig, set):
            raise WrongType
        else:
            member = request[3]
            if member in orig:
                # we my be able to move
                if dest is None:
                    dest = set()
                    self._data[request[2]] = dest
                elif not isinstance(dest, set):
                    raise WrongType
                orig.remove(member)
                dest.add(member)
                result = 1
            else:
                result = 0
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def spop(self, connection, request):
        N = len(request) - 1
        check_input('spop', N, N != 1, 'expects one argument')
        value = self._get(request[1])
        if value is None:
            result = None
        elif not isinstance(value, set):
            raise WrongType
        else:
            try:
                result = value.pop()
            except KeyError:
                result = None
            if not value:
                self._data.pop(request[1])
        connection._transport.write(self._parser.bulk(result))

    def srandmember(self, connection, request):
        N = len(request) - 1
        check_input('srandmember', N, N < 1 or N > 2,
                    'expects at least one argument')
        value = self._get(request[1])
        if value is None:
            result = None
        elif not isinstance(value, set):
            raise WrongType
        if N == 2:
            count = int(request[1])
            if count < 0:
                count = -count
                if not value:
                    result = (None,) * count
                else:
                    result = []
                    for _ in range(count):
                        el = value.pop()
                        result.append(el)
                        value.add(el)
            elif count > 0:
                if not value:
                    result = (None,)
                elif len(value) <= count:
                    result = list(value)
                    result.extend((None,)*(count-len(value)))
                else:
                    result = []
                    for _ in range(count):
                        el = value.pop()
                        result.append(el)
                    value.update(result)
            else:
                result = []
            connection._transport.write(self._parser.multi_bulk(result))
        else:
            if not value:
                result = None
            else:
                result = value.pop()
                value.add(result)
            connection._transport.write(self._parser.bulk(result))

    def srem(self, connection, request):
        N = len(request) - 1
        check_input('srem', N, N < 2, 'expects at least two arguments')
        value = self._get(request[1])
        if value is None:
            result = 0
        elif not isinstance(value, set):
            raise WrongType
        else:
            members = request[2:]
            result = reduce(lambda x, y: x + int(y in value), members)
            value.difference_update(members)
            if not value:
                self._data.pop(request[1])
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))

    def sunion(self, connection, request):
        N = len(request) - 1
        check_input('sunion', N, N < 1, 'expects at least one argument')
        self._setoper(connection, 'union', request[1:])

    def sunionstore(self, connection, request):
        N = len(request) - 1
        check_input('sunionstore', N, N < 2, 'expects at least two argument')
        self._setoper(connection, 'union', request[2:], request[1])

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
        chunk = self._parser.bulk(request[1])
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
            value = self._parser.multi_bulk(tuple(self._client_list))
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
        chunk = self._parser.bulk(info.encode('utf-8'))
        connection._transport.write(chunk)

    def time(self, connection, request):
        if len(request) != 1:
            raise CommandError("'time' expect no arguments")
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        info = '\n'.join(self._flat_info())
        chunk = self._parser.multi_bulk(seconds, microseconds)
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

    def _set(self, key, value):
        self._data[key] = value
        handles = self._key_space_events.pop(key, None)
        if handles:
            for handle in handles:
                if not handle.canceled:
                    handle.callback(*handle.args)

    def _remove_value(self, key, value):
        if not value:
            self._data.pop(key, None)
            if key in self._expires:
                self._expires.pop(key).cancel()

    def wait_for_keys(self, connection, keys, timeout, callback, *args):
        bpop = connection.bpop
        connection.bpop = bpop = TimeHandle(timeout, callback, args)
        for key in keys:
            if key not in bpop.keys:
                clients = self._blocking_keys.get(key)
                if clients is None:
                    clients = []
                    self._blocking_keys[key] = clients
                clients.append(connection)
        connection.flags |= REDIS_BLOCKED
        self._storage._bpop_blocked_clients += 1

    def _list_push(self, connection, request, name):
        N = len(request) - 1
        check_input(name, N, N < 2, 'expects at least two arguments')
        key = request[1]
        value = self._get(key)
        if value is None:
            value = self.list_type()
            self._data[key] = value
        elif not isinstance(value, self.list_type):
            raise WrongType
        if name == 'lpush':
            value.extend_left(requests[2:])
        else:
            value.extend(requests[2:])
        result = len(value)
        self._storage._dirty += N - 1
        connection._transport.write((':%d\r\n' % result).encode('utf-8'))
        self._storage._signal_modified_key(self, key)
        self._storage._notifyKeyspaceEvent(REDIS_NOTIFY_LIST, name, self, key)

    def _flush(self):
        timeouts = list(self._expires.values())
        self._data.clear()
        self._expires.clear()
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
                'expires': len(self._expires)}

    def _encode_info_value(self, value):
        return str(value).replace('=', ' ').replace(',', ' ')

    def _hincrby(self, name, request, type):
        N = len(request) - 1
        check_input(name, N, N != 3, 'expects three arguments')
        key = request[1]
        field = request[2]
        increment = type(request[3])
        hash = self._get(key)
        if hash is None:
            self._data[key] = self.hashtype(field=increment)
            result = increment
        elif getattr(hash, 'datatype', 'hash'):
            if not field in hash:
                hash[field] = increment
                result = increment
            else:
                result = type(hash[field]) + increment
                hash[field] = result
        else:
            raise WrongType
        return result

    def _setoper(self, connection, oper, keys, dest=None):
        result = None
        for key in keys:
            value = self._get(key)
            if value is None:
                value = set()
            elif not isinstance(value, set):
                raise WrongType
            if result is None:
                result = value
            else:
                result = getattr(result, oper)(value)
        if dest is not None:
            if result:
                self._data[dest] = result
            else:
                self._data.pop(dest, None)
            result = len(result)
            connection._transport.write((':%d\r\n' % result).encode('utf-8'))
        else:
            connection._transport.write(self._parser.multi_bulk(result))


class Storage(object):

    def __init__(self, server, databases, password=None):
        self._password = password
        self._server = server
        self._loop = server._loop
        self._parser = server._parser_class()
        self._missed_keys = 0
        self._hit_keys = 0
        self._bpop_blocked_clients = 0
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

    def _signal_modified_key(self, db, key):
        pass

    def _notifyKeyspaceEvent(self, type, event, db, key):
        for handle in KEYSPACE_EVENT_HANDLES[type]:
            handle(event, db, key)

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
        chunk = self._parser.multi_bulk(*channels)
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
        msg = self._parser.multi_bulk(b'message', channel, message)
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
