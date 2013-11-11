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
NOTIFY_KEYSPACE = (1<<0)
NOTIFY_KEYEVENT = (1<<1)
NOTIFY_GENERIC = (1<<2)
NOTIFY_STRING = (1<<3)
NOTIFY_LIST = (1<<4)
NOTIFY_SET = (1<<5)
NOTIFY_HASH = (1<<6)
NOTIFY_ZSET = (1<<7)
NOTIFY_EXPIRED = (1<<8)
NOTIFY_EVICTED = (1<<9)
NOTIFY_ALL = (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET |
              NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED)

MULTI  = (1 << 3)
BLOCKED = (1 << 4)

DEL = ('del',)
EXPIRE = ('expire',)
SREM = ('srem',)
SADD = ('sadd',)


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


class PulsarStoreClient(pulsar.Protocol):
    '''Used both by client and server'''
    bpop = None
    flag = 0
    def __init__(self, cfg, *args, **kw):
        super(PulsarStoreClient, self).__init__(*args, **kw)
        self.cfg = cfg
        self.parser = self._producer._parser_class()
        self.store = self._producer._key_value_store
        self.database = 0
        self.channels = None
        self.patterns = None
        self.transaction = None
        self.password = ''

    @property
    def db(self):
        return self.store.databases[self.database]

    def int_reply(self, value):
        self.write((':%d\r\n' % value).encode('utf-8'))

    def write(self, response):
        if self.transaction is not None:
            self.transaction.append(response)
        else:
            self._transport.write(response)

    def data_received(self, data):
        self.parser.feed(data)
        request = self.parser.get()
        if request is not False:
            server = self._producer
            try:
                command = request[0].decode('utf-8').lower()
                name = COMMAND_NAMES.get(command, command)
                handle = getattr(server._key_value_store, name, None)
                if not handle:
                    raise CommandError("unknown command '%s'" % command)
                if server.password != self.password:
                    if command != 'auth':
                        raise CommandError("not authenticated")
                handle(self, request, len(request) - 1)
            except CommandError as e:
                msg = '-%s %s\r\n' % (e.error_type, e)
                self.write(msg.encode('utf-8'))
            except Exception as e:
                msg = '-SERVERERR %s\r\n' % e
                self.write(msg.encode('utf-8'))
                raise


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
        return partial(PulsarStoreClient, self.cfg)

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


def check_input(request, failed):
    if failed:
        raise CommandError("wrong number of arguments for '%s'" % request[0])


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
        self._event_handlers = {}
        self._set_options = (b'ex', b'px', 'nx', b'xx')
        self.OK = b'+OK\r\n'
        self.PONG = b'+PONG\r\n'
        self.ZERO = b':0\r\n'
        self.ONE = b':1\r\n'
        self.NIL = b'$-1\r\n'
        self.EMPTY_ARRAY = b'*0\r\n'
        self.WRONG_TYPE = (b'-WRONGTYPE Operation against a key holding '
                           b'the wrong kind of value\r\n')
        self.hashtype = Hash
        self.list_type = list
        self.databases = dict(((num, Db(num, self))
                               for num in range(databases)))

    ###########################################################################
    ##    KEYS COMMANDS
    @command_name('del')
    def delete(self, connection, request, N):
        check_input(request, not N)
        pop = connection.db.pop
        result = reduce(lambda x, y: x + pop(key), requests[1:])
        connection.int_reply(result)

    def exists(self, connection, request, N):
        check_input(request, N != 1)
        if connection.db.exists(request[1]):
            connection.write(self.ONE)
        else:
            connection.write(self.ZERO)

    def expire(self, connection, request, N):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            raise CommandError('invalid expire time')
        if timeout:
            if timeout < 0:
                raise CommandError('invalid expire time')
            if connection.db.expire(request[1], timeout):
                return connection.write(self.ONE)
        connection.write(self.ZERO)

    ###########################################################################
    ##    STRING COMMANDS
    def append(self, connection, request, N):
        check_input(request, N != 2,)
        key = request[1]
        value = self._data.get(key)
        if value is None:
            value = request[2]
            self._data[key] = value
        elif not isinstance(value, bytes):
            raise WrongType
        else:
            value = value + request[2]
            self._data[key] = value
        self._storage._signal(NOTIFY_STRING, request, self, key, 1)
        connection.int_reply(len(value))

    def get(self, connection, request, N):
        check_input(request, N != 1)
        value = self._get(request[1])
        connection.write(self._parser.bulk(value))

    def getset(self, connection, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if key in self._expires:
            self._expires.pop(key).cancel()
        self._data[key] = request[2]
        self._storage._signal(NOTIFY_STRING, request, self, key, 1)
        connection.write(self._parser.bulk(value))

    def mget(self, connection, request, N):
        check_input(request, not N)
        get = self._get
        values = tuple((get(k) for k in request[1:]))
        connection.write(self._parser.multi_bulk(*values))

    def set(self, connection, request, N):
        check_input(request, N < 2 or N > 8)
        db = connection.db
        key, value = request[1], request[2]
        it = 2
        extra = set(self._set_options)
        seconds = 0
        milliseconds = 0
        nx = False
        xx = False
        while N > it:
            it += 1
            opt = request[it].lower()
            if opt in extra:
                extra.remove(opt)
                if opt == b'ex':
                    it += 1
                    seconds = int(request[it])
                elif opt == b'px':
                    it += 1
                    milliseconds = 0.000001*int(request[it])
                elif opt == b'nx':
                    nx = True
                else:
                    xx = True
        if seconds < 0 or milliseconds < 0:
            raise CommandError('invalid expire time')
        timeout = seconds + milliseconds
        exists = db.exists(key)
        skip = (exists and nx) or (not exists and xx)
        if not skip:
            db.pop(key, notify=False)
            if key in self._expires:
                self._expires.pop(key).cancel()
            if timeout > 0:
                self._expires[key] = self._loop.call_later(
                    timeout, self._pop, key)
                self._storage._signal(NOTIFY_STRING, EXPIRE, self, key)
            self._data[key] = value
            self._storage._signal(NOTIFY_STRING, request, self, key, 1)
        connection.write(self.OK)

    ###########################################################################
    ##    HASHES COMMANDS
    def hdel(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        value = self._get(key)
        if value is None:
            rem = 0
        elif getattr(value, 'datatype', 'hash'):
            for field in request[2:]:
                rem += 0 if value.pop(field, None) is None else 1
            self._storage._signal(NOTIFY_HASH, request, self, key, rem)
            if self._pop(key, value):
                self._storage._signal(NOTIFY_GENERIC, DEL, self, key)
        else:
            raise WrongType
        connection.int_reply(rem)

    def hexists(self, connection, request, N):
        check_input(request, N != 2)
        value = self._get(request[1])
        if value is None:
            rem = 0
        elif getattr(value, 'datatype', 'hash'):
            rem = int(request[2] in value)
        else:
            raise WrongType
        connection.int_reply(rem)

    def hget(self, connection, request, N):
        check_input(request, N != 2)
        value = self._get(request[1])
        if value is None:
            result = None
        elif getattr(value, 'datatype', 'hash'):
            result = value.get(request[2])
        else:
            raise WrongType
        connection.write(self._parser.bulk(result))

    def hgetall(self, connection, request, N):
        check_input(request, N != 1)
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = value.flat()
        else:
            raise WrongType
        connection.write(self._parser.multi_bulk(result))

    def hincrby(self, connection, request):
        result = self._hincrby('hincrby', request, int)
        connection.int_reply(result)

    def hincrbyfloat(self, connection, request):
        result = self._hincrby('hincrbyfloat', request, float)
        result = str(result).encode('utf-8')
        connection.write(self._parser.bulk(result))

    def hkeys(self, connection, request, N):
        check_input(request, N != 1)
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = tuple(value)
        else:
            raise WrongType
        connection.write(self._parser.multi_bulk(result))

    def hlen(self, connection, request, N):
        check_input(request, N != 1)
        value = self._get(request[1])
        if value is None:
            result = 0
        elif getattr(value, 'datatype', 'hash'):
            result = len(value)
        else:
            raise WrongType
        connection.int_reply(result)

    def hmget(self, connection, request, N):
        check_input(request, N < 3)
        value = self._get(request[1])
        if value is None:
            result = self.EMPTY_ARRAY
        elif getattr(value, 'datatype', 'hash'):
            result = value.mget(request[2:])
            result = self._parser.multi_bulk(result)
        else:
            result = self.WRONG_TYPE
        connection.write(result)

    def hmset(self, connection, request, N):
        check_input(request, N < 3 or (N-1) // 2 * 2 == N-1)
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        value.update(zip(request[2::2], request[3::2]))
        connection.write(self.OK)

    def hset(self, connection, request, N):
        check_input(request, N != 3)
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        result = int(request[3] in value)
        value[request[2]] = request[3]
        connection.int_reply(result)

    def hsetnx(self, connection, request, N):
        check_input(request, N != 3)
        value = self._get(request[1])
        if value is None:
            value = self.hashtype()
            self._data[request[1]] = value
        elif not getattr(value, 'datatype', 'hash'):
            raise WrongType
        result = int(request[3] in value)
        if not result:
            value[request[2]] = request[3]
        connection.int_reply(result)

    def hvals(self, connection, request, N):
        check_input(request, N != 1)
        value = self._get(request[1])
        if value is None:
            result = ()
        elif getattr(value, 'datatype', 'hash'):
            result = tuple(value.values())
        else:
            raise WrongType
        connection.write(self._parser.multi_bulk(result))

    ###########################################################################
    ##    LIST COMMANDS
    def blpop(self, connection, request, N):
        check_input(request, N < 2)
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._lpop(connection, request, keys, 'pop_left'):
            self.wait_for_keys(timeout, keys, self._lpop, connection, keys,
                               'pop_left')

    def brpop(self, connection, request, N):
        check_input(request, N < 2)
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._lpop(connection, request, keys, 'pop'):
            self.wait_for_keys(timeout, keys, self._lpop, connection, keys,
                               'pop')

    def lindex(self, connection, request, N):
        check_input(request, N != 2)
        value = connection.db.get(request[1])
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
        connection.write(self._parser.bulk(result))

    def llen(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            result = 0
        elif isinstance(value, self.list_type):
            result = len(value)
        else:
            raise WrongType
        connection.int_reply(result)

    def lpush(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.list_type()
            db._data[key] = value
        elif not isinstance(value, self.list_type):
            raise WrongType
        if request[0] == 'lpush':
            value.extend_left(requests[2:])
        else:
            value.extend(requests[2:])
        connection.int_reply(len(value))
        self._signal(NOTIFY_LIST, connection, request, key, N - 1)
    rpush = lpush

    def _lpop(self, connection, request, keys, method):
        list_type = self.list_type
        for key in keys:
            value = self._get(key)
            if isinstance(value, list_type):
                result = (key, geattr(value, mehod)())
                self._storage._signal(NOTIFY_LIST, connection, request, key, 1)
                if self._pop(key, value):
                    self._storage._signal(NOTIFY_GENERIC, connection, DEL, key)
                connection.write(self._parser.multi_bulk(result))
                return True
            elif value is not None:
                connection.write(self.WRONG_TYPE)
        return False

    ###########################################################################
    ##    SETS COMMANDS
    def sadd(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = set()
            self._data[key] = value
        elif not isinstance(value, set):
            return connection.write(self.WRONG_TYPE)
        n = len(value)
        value.update(request[2:])
        n = len(value) - n
        self._signal(NOTIFY_SET, connection, request, key, n)
        connection.int_reply(n)

    def scard(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.int_reply(len(value))

    def sdiff(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'difference', request[1:])

    def sdiffstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'difference', request[2:], request[1])

    def sinter(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'intersection', request[1:])

    def sinterstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'intersection', request[2:], request[1])

    def sismemeber(self, connection, request, N):
        check_input(request, N != 2)
        value = connection.db(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.int_reply(int(request[2] in value))

    def smembers(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.write(self._parser.multi_bulk(value))

    def smove(self, connection, request, N):
        check_input(request, N != 3)
        db = connection.db
        key1 = request[1]
        key2 = request[2]
        orig = db.get(key1)
        dest = db.get(key2)
        if orig is None:
            connection.write(self.ZERO)
        elif not isinstance(orig, set):
            connection.write(self.WRONG_TYPE)
        else:
            member = request[3]
            if member in orig:
                # we my be able to move
                if dest is None:
                    dest = set()
                    db._data[request[2]] = dest
                elif not isinstance(dest, set):
                    return connection.write(self.WRONG_TYPE)
                orig.remove(member)
                dest.add(member)
                self._signal(NOTIFY_SET, connection, SREM, key1)
                self._signal(NOTIFY_SET, connection, SADD, key2, 1)
                if not orig:
                    db.raw_pop(key1)
                    self._signal(NOTIFY_GENERIC, connection, DEL, key1)
                connection.write(self.ONE)
            else:
                connection.write(self.ZERO)

    def spop(self, connection, request, N):
        check_input(request, N != 1)
        key = request[1]
        value = connection.db.get(key)
        if value is None:
            connection.write(self.NIL)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            result = value.pop()
            self._signal(NOTIFY_SET, connection, request, key, 1)
            if not value:
                db.raw_pop(key)
                self._signal(NOTIFY_GENERIC, connection, DEL, key)
            connection.write(self._parser.bulk(result))

    def srandmember(self, connection, request, N):
        check_input(request, N < 1 or N > 2)
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
            connection.write(self._parser.multi_bulk(result))
        else:
            if not value:
                result = None
            else:
                result = value.pop()
                value.add(result)
            connection.write(self._parser.bulk(result))

    def srem(self, connection, request, N):
        check_input(request, N < 2)
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
        connection.write((':%d\r\n' % result).encode('utf-8'))

    def sunion(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'union', request[1:])

    def sunionstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'union', request[2:], request[1])

    ###########################################################################
    ##    PUBSUB COMMANDS
    def psubscribe(self, connection, request, N):
        check_input(request, not N)
        self._storage._psubscribe(connection, request[1:])

    def pubsub(self, connection, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'channels':
            if N == 2:
                pattern = request[2]
            elif N > 2:
                check_input(request, False)
            else:
                pattern = None
            self._storage._pubsub_channels(connection, pattern)
        elif subcommand == 'numsub':
            self._storage._pubsub_numsub(connection, request[2:])
        elif subcommand == 'numpat':
            check_input(request, N > 1)
            self._storage._pubsub_numpat(connection, request[2:])
        else:
            raise CommandError("unknown command 'pubsub %s'" % subcommand)

    def publish(self, connection, request, N):
        check_input(request, N != 2)
        self._storage._publish(connection, request[1], request[2])

    def punsubscribe(self, connection, request, N):
        check_input(request, not N)
        self._storage._punsubscribe(connection, request[1:])

    def subscribe(self, connection, request, N):
        check_input(request, not N)
        self._storage._subscribe(connection, request[1:])

    def unsubscribe(self, connection, request, N):
        check_input(request, not N)
        self._storage._unsubscribe(connection, request[1:])

    ###########################################################################
    ##    CONNECTION COMMANDS
    def execute(self, connection, request, N):
        check_input(request, not N)
        connection.transaction = []
        connection.transaction.append(ok)

    def multi(self, connection, request, N):
        check_input(request, not N)
        connection.transaction = []
        connection.transaction.append(ok)

    ###########################################################################
    ##    CONNECTION COMMANDS
    def auth(self, connection, request, N):
        check_input(request, N != 1)
        conn.password = request[1].encode('utf-8')
        if conn.password != conn._producer.password:
            raise CommandError("wrong password")
        connection.write(self.OK)

    def echo(self, connection, request, N):
        check_input(request, N != 1)
        connection.write(self._parser.bulk(request[1]))

    def ping(self, connection, request):
        check_input(request, not N)
        connection.write(self.PONG)

    def quit(self, connection, request):
        if len(request) != 1:
            raise CommandError("'quit' expect no arguments")
        connection.write(self.OK)
        connection.close()

    def select(self, connection, request, N):
        check_input(request, N != 1)
        D = len(self.databases) - 1
        try:
            num = int(request[1])
            if num < 0 or num > D:
                raise ValueError
        except ValueError:
            raise CommandError(
                'select requires a database number between %s and %s' % (0, D))
        connection.database = num
        connection.write(self.OK)

    ###########################################################################
    ##    SERVER COMMANDS
    def client(self, connection, request, N):
        check_input(request, N < 2)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'list':
            if len(request) > 2:
                raise CommandError("'client list' no arguments")
            value = self._parser.multi_bulk(tuple(self._client_list))
        else:
            raise CommandError("unknown command 'client %s'" % subcommand)
        connection.write(value)

    def dbsize(self, connection, request, N):
        check_input(request, N != 0)
        connection.int_reply(len(connection.db))

    def flushdb(self, connection, request, N):
        check_input(request, N != 0)
        connection.db.flush()
        connection.write(self.OK)

    def flushall(self, connection, request, N):
        check_input(request, N != 0)
        for db in self.databases.values():
            db.flush()
        connection.write(self.OK)

    def info(self, connection, request, N):
        check_input(request, N != 0)
        info = '\n'.join(self._flat_info())
        chunk = self._parser.bulk(info.encode('utf-8'))
        connection.write(chunk)

    def time(self, connection, request, N):
        check_input(request, N != 0)
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        chunk = self._parser.multi_bulk(seconds, microseconds)
        connection.write(chunk)

    ###########################################################################
    ##    INTERNALS
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
        connection.flags |= BLOCKED
        self._storage._bpop_blocked_clients += 1

    def _flat_info(self):
        info = self._server.info()
        info['server']['redis_version'] = '2.4.10'
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
        return str(value).replace('=',
                                  ' ').replace(',',
                                               ' ').replace('\n', ' - ')

    def _hincrby(self, name, request, type, N):
        check_input(request, N != 3)
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
        db = connection.db
        result = None
        for key in keys:
            value = db.get(key)
            if value is None:
                value = set()
            elif not isinstance(value, set):
                return connection.write(self.WRONG_TYPE)
            if result is None:
                result = value
            else:
                result = getattr(result, oper)(value)
        if dest is not None:
            db._raw_pop(dest)
            if result:
                db._data[dest] = result
                connection.int_reply(len(result))
            else:
                connection.write(self.ZERO)
        else:
            connection.write(self._parser.multi_bulk(result))

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

    def _signal(self, type, connection, request, key, dirty=0):
        self._dirty += dirty
        event = request[0]
        handles = self._event_handlers.get(type)
        if handles:
            for handle in handles:
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
        connection.write(chunk)

    def _pubsub_numsub(self, connection, channels):
        count = 0
        for channel in channels:
            clients = self._channels.get(channel, ())
            count += len(clients)
        connection.write((':%d\r\n' % count).encode('utf-8'))

    def _pubsub_numpat(self, connection):
        count = reduce(lambda x, y: x + len(y.clients),
                       self._patterns.values())
        connection.write((':%d\r\n' % count).encode('utf-8'))

    def _publish(self, connection, channel, message):
        ch = channel.decode('utf-8')
        clients = self._channels.get(channel)
        msg = self._parser.multi_bulk(b'message', channel, message)
        count = self.__publish_clients(msg, self._channels.get(channel, ()))
        for pattern in self._patterns.values():
            g = pattern.re.match(ch)
            if g:
                count += self.__publish_clients(msg, pattern.clients)
        connection.write((':%d\r\n' % count).encode('utf-8'))

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
            connection.write(chunk)

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
                client.write(msg)
                count += 1
            except Exception:
                remove.append(client)
        if remove:
            clients.difference_update(remove)
        return count


class Db(object):
    '''The database.
    '''
    def __init__(self, num, storage):
        self._num = num
        self._storage = storage
        self._data = {}
        self._expires = {}
        self._events = {}

    def __repr__(self):
        return 'db%s' % self._num
    __str__ = __repr__

    def __len__(self):
        return len(self._data) + len(self._expires)

    ###########################################################################
    ##    INTERNALS
    def flush(self):
        self._data.clear()
        [handle.cancel() for handle, _ in self._expires.values()]
        self._expires.clear()

    def get(self, key, default=None):
        if key in self._data:
            self._storage._hit_keys += 1
            return self._data[key]
        elif key in self._expires:
            self._storage._hit_keys += 1
            return self._expires[key]
        else:
            self._storage._missed_keys += 1
            return default

    def exists(self, key):
        return key in self._data or key in self._expires

    def expire(self, key, timeout):
        if key in self._expires:
            handle, value = self._expires.pop(key)
            handle.cancel()
        elif key in self._data:
            value = self._data.pop(key)
        else:
            return False
        self._expires[key] = (self._loop.call_later(
            timeout, self.pop, key), value)
        return True

    def pop(self, key, value=None, dirty=1):
        if not value:
            st = self._storage
            if self.raw_pop(key) is None:
                st._missed_keys += 1
                return 0
            else:
                st._hit_keys += 1
                st._signal(NOTIFY_GENERIC, DEL, self, key, dirty)
                return 1
        return 0

    def raw_pop(self, key):
        if key in self._data:
            value = self._data.pop(key)
            return value
        elif key in self._expires:
            handle, value, self._expires.pop(key).cancel()
            handle.cancel()
            return value
        return None
