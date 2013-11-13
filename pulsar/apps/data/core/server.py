'''Classes for the pulsar Key-Value store server
'''
import os
import re
import time
import math
from random import choice
from itertools import islice, chain
from functools import partial, reduce
from collections import namedtuple

import pulsar
from pulsar.apps.socket import SocketServer
from pulsar.utils.config import Global
from pulsar.utils.structures import Hash, Zset, Deque
from pulsar.utils.pep import map, range, zip, pickle


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

MONITOR = (1 << 2)
MULTI  = (1 << 3)
BLOCKED = (1 << 4)
DIRTY_CAS = (1 << 5)

STRING_LIMIT = 2**32

nan = float('nan')


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


def validate_list_of_pairs(val):
    new_val = []
    if val:
        if not isinstance(val, (list, tuple)):
            raise TypeError("Not a list: %s" % val)
        for elem in val:
            if not isinstance(elem, (list, tuple)):
                raise TypeError("Not a list: %s" % elem)
            if not len(elem) == 2:
                raise TypeError("Not a pair: %s" % str(elem))
            new_val.append((int(elem[0]), int(elem[1])))
    return new_val

###############################################################################
##    CONFIGURATION PARAMETERS

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


class KeyValueSave(KeyValuePairSetting):
    name = "key_value_save"
    default = [(900, 1), (300, 10), (60, 10000)]
    validator = validate_list_of_pairs
    desc=''''
        Will save the DB if both the given number of seconds and the given
        number of write operations against the DB occurred.

        The default behaviour will be to save:
        after 900 sec (15 min) if at least 1 key changed
        after 300 sec (5 min) if at least 10 keys changed
        after 60 sec if at least 10000 keys changed

        You can disable saving at all by setting an ampty list
    '''


class KeyValueFileName(KeyValuePairSetting):
    name = "key_value_filename"
    flags = ["--key-value-filename"]
    default='pulsarkv.rdb'
    desc='The filename where to dump the DB.'


class StoreError(Exception):
    pass


class CommandError(StoreError):
    error_type = 'ERR'
    message = None

    def __init__(self, message=None, error_type=None):
        super(CommandError, self).__init__(message or self.message)
        if error_type:
            self.error_type = error_type


class WrongType(CommandError):
    error_type = 'WRONGTYPE'
    message = 'Operation against a key holding the wrong kind of value'


class PulsarStoreClient(pulsar.Protocol):
    '''Used both by client and server'''

    def __init__(self, cfg, *args, **kw):
        super(PulsarStoreClient, self).__init__(*args, **kw)
        self.cfg = cfg
        self.parser = self._producer._parser_class()
        self.store = self._producer._key_value_store
        self.flag = 0
        self.started = time.time()
        self.database = 0
        self.channels = set()
        self.patterns = set()
        self.transaction = None
        self.watched_keys = None
        self.blocked = None
        self.password = b''
        self.last_command = ''
        clean = partial(self.store._remove_connection, self)
        self.bind_event('connection_lost', clean, clean)

    @property
    def db(self):
        return self.store.databases[self.database]

    def int_reply(self, value):
        self.write((':%d\r\n' % value).encode('utf-8'))

    def error_reply(self, value):
        self.write(('-ERR %s\r\n' % value).encode('utf-8'))

    def write(self, response):
        if self.transaction is not None:
            self.transaction.append(response)
        else:
            self._transport.write(response)

    def data_received(self, data):
        self.parser.feed(data)
        request = self.parser.get()
        while request is not False:
            self._execute(request)
            request = self.parser.get()

    def _execute(self, request):
        store = self.store
        if store._monitors:
            store._write_to_monitors(connection, request)
        handle = None
        if request:
            request[0] = command = request[0].decode('utf-8').lower()
            info = COMMANDS_INFO.get(command)
            if info:
                handle = getattr(self.store, info.method_name)
        #
        flag = self.flag
        if self.channels or self.patterns:
            if command not in self.store.SUBSCRIBE_COMMANDS:
                self.write(self.store.PUBSUB_ONLY)
                return
        if self.blocked:
            self.write(self.store.BLOCKED)
            return
        if self.transaction is not None and command not in 'exec':
            self.transaction.append((handle, request))
            self._transport.write(self.store.QUEUED)
            return
        self.execute(handle, request)

    def execute(self, handle, request):
        try:
            if request:
                command = request[0]
                if not handle:
                    return self.error_reply("unknown command '%s'" % command)
                if self.store._password != self.password:
                    if command != 'auth':
                        return self.write(self.store.NOAUTH)
                handle(self, request, len(request) - 1)
            else:
                command = ''
                return self.error_reply("no command")
        except CommandError as e:
            msg = '-%s %s\r\n' % (e.error_type, e)
            self.write(msg.encode('utf-8'))
        except Exception:
            self._loop.logger.exception("Server error on '%s' command",
                                        command)
            self.write(self.store.SERVER_ERROR)
        finally:
            self.last_command = command


class Blocked:

    def __init__(self, connection, timeout):
        self.keys = set()
        if timeout:
            self.handle = connection._loop.call_later(timeout, self.unblock,
                                                      connection)
        else:
            self.handle = None

    def unblock(self, connection):
        if self.handle:
            self.handle.cancel()
        connection.blocked = None
        connection.write(connection.store.EMPTY_ARRAY)


class TcpServer(pulsar.TcpServer):

    def __init__(self, cfg, *args, **kwargs):
        super(TcpServer, self).__init__(*args, **kwargs)
        self.cfg = cfg
        self._parser_class = redis_parser(cfg.py_redis_parser)
        self._key_value_store = Storage(self, cfg)

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
COMMANDS_INFO = {}


class command:

    def __init__(self, group, write=False, name=None):
        self.group = group
        self.write = write
        self.name = name

    def __call__(self, f):
        self.method_name = f.__name__
        if not self.name:
            self.name = self.method_name
        COMMANDS_INFO[self.name] = self
        f._info = self
        return f


def check_input(request, failed):
    if failed:
        raise CommandError("wrong number of arguments for '%s'" % request[0])


class Storage(object):

    def __init__(self, server, cfg):
        self.cfg = cfg
        self._password = cfg.key_value_password.encode('utf-8')
        self._filename = cfg.key_value_filename
        self._server = server
        self._loop = server._loop
        self._parser = server._parser_class()
        self._missed_keys = 0
        self._hit_keys = 0
        self._expired_keys = 0
        self._dirty = 0
        self._bpop_blocked_clients = 0
        self._last_save = int(time.time())
        self._channels = {}
        self._patterns = {}
        # The set of connections which are watching keys
        self._watching = set()
        # The set of connections which issued the monitor command
        self._monitors = set()
        self._event_handlers = {NOTIFY_GENERIC: self._generic_event,
                                NOTIFY_STRING: self._string_event,
                                NOTIFY_SET: self._set_event,
                                NOTIFY_HASH: self._hash_event,
                                NOTIFY_LIST: self._list_event,
                                NOTIFY_ZSET: self._zset_event}
        self._set_options = (b'ex', b'px', 'nx', b'xx')
        self._unblock_commands = ('lpush', 'lpushx', 'rpush', 'rpushx')
        self.OK = b'+OK\r\n'
        self.PONG = b'+PONG\r\n'
        self.QUEUED = b'+QUEUED\r\n'
        self.ZERO = b':0\r\n'
        self.ONE = b':1\r\n'
        self.NIL = b'$-1\r\n'
        self.NULL_ARRAY = b'*-1\r\n'
        self.EMPTY_ARRAY = b'*0\r\n'
        self.WRONG_TYPE = (b'-WRONGTYPE Operation against a key holding '
                           b'the wrong kind of value\r\n')
        self.NOAUTH = b'-NOAUTH Authentication required\r\n'
        self.SERVER_ERROR = b'-ERR Server error\r\n'
        self.INVALID_TIMEOUT = b'-ERR invalid expire time'
        self.PUBSUB_ONLY = (b'-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT '
                            b'allowed in this context\r\n')
        self.BLOCKED = b'-ERR blocked client cannot request\r\n'
        self.INVALID_SCORE = b'-ERR Invalid score value\r\n'
        self.OUT_OF_BOUND = b'-ERR Out of bound\r\n'
        self.SYNTAX_ERROR = b'-ERR Syntax error\r\n'
        self.SUBSCRIBE_COMMANDS = ('psubscribe', 'punsubscribe', 'subscribe',
                                   'unsubscribe', 'quit')
        self.hash_type = Hash
        self.list_type = Deque
        self.zset_type = Zset
        self._type_event_map = {bytearray: NOTIFY_STRING,
                                self.hash_type: NOTIFY_HASH,
                                self.list_type: NOTIFY_LIST,
                                set: NOTIFY_SET,
                                self.zset_type: NOTIFY_ZSET}
        self._type_name_map = {bytearray: 'string',
                               self.hash_type: 'hash',
                               self.list_type: 'list',
                               set: 'set',
                               self.zset_type: 'zset'}
        self.databases = dict(((num, Db(num, self))
                               for num in range(cfg.key_value_databases)))
        self._loaddb()
        self._loop.call_repeatedly(1, self._cron)

    ###########################################################################
    ##    KEYS COMMANDS
    @command('keys', True, name='del')
    def delete(self, connection, request, N):
        check_input(request, not N)
        rem = connection.db.rem
        result = reduce(lambda x, y: x + rem(y), request[1:], 0)
        connection.int_reply(result)

    @command('keys')
    def exists(self, connection, request, N):
        check_input(request, N != 1)
        if connection.db.exists(request[1]):
            connection.write(self.ONE)
        else:
            connection.write(self.ZERO)

    @command('keys', True)
    def expire(self, connection, request, N):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            connection.write(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return connection.write(self.INVALID_TIMEOUT)
                if connection.db.expire(request[1], timeout):
                    return connection.write(self.ONE)
            connection.write(self.ZERO)

    @command('keys', True)
    def expireat(self, connection, request, N):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            connection.write(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return connection.write(self.INVALID_TIMEOUT)
                if connection.db.expireat(request[1], timeout):
                    return connection.write(self.ONE)
            connection.write(self.ZERO)

    @command('keys')
    def keys(self, connection, request, N):
        err = 'ignore'
        check_input(request, N != 1)
        pattern = request[1].decode('utf-8', err)
        allkeys = pattern == '*'
        gr = None
        if not allkeys:
            gr = re.compile(pattern)
        result = [key for key in connection.db if allkeys or
                  gr.search(key.decode('utf-8', err))]
        connection.write(self._parser.multi_bulk(*result))

    @command('keys', True)
    def move(self, connection, request, N):
        check_input(request, N != 2)
        key = request[1]
        try:
            db2 = self.databases.get(int(request[2]))
            if not db:
                raise ValueError
        except Exception:
            return connection.error_reply('Invalid database')
        db = connection.db
        value = db.get(key)
        if db2.exists(key) or value is None:
            return connection.write(self.ZERO)
        db.pop(key)
        self._signal(NOTIFY_GENERIC, db, 'del', key, 1)
        db2._data[key] = value
        self._signal(self._type_event_map[type(value)], db2, 'set', key, 1)
        connection.write(self.ONE)

    @command('keys', True)
    def persist(self, connection, request, N):
        check_input(request, N != 1)
        if connection.db.persist(request[1]):
            connection.write(self.ONE)
        else:
            connection.write(self.ZERO)

    @command('keys')
    def randomkey(self, connection, request, N):
        check_input(request, N)
        keys = list(connection.db)
        if keys:
            connection.write(self._parser.bulk(choice(keys)))
        else:
            connection.write(self.NIL)

    @command('keys', True)
    def rename(self, connection, request, N, ex=False):
        check_input(request, N != 2)
        key1, key2 = request[1], request[2]
        db = connection.db
        value = db.get(key1)
        if value is None:
            connection.error_reply('Cannot rename key, not available')
        elif key1 == key2:
            connection.error_reply('Cannot rename key')
        else:
            if ex:
                if db.exists(key2):
                    return connection.write(self.ZERO)
                result = self.ONE
            else:
                result = self.OK
                db.rem(key2)
            db.pop(key1)
            event = self._type_event_map[type(value)]
            dirty = 1 if event == NOTIFY_STRING else len(value)
            db._data[key2] = value
            self._signal(event, db, request[0], key2, dirty)
            connection.write(result)

    @command('keys', True)
    def renameex(self, connection, request, N):
        self.rename(connection, request, N, True)

    @command('sort', True)
    def sort(self, connection, request, N):
        check_input(request, not N)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            pass
        elif not isinstance(value, (set, self.list_type, self.zset_type)):
            return connection.write(self.WRONG_TYPE)
        sort_type = type(value)
        stype = self._type_event_map[sort_type]
        right = 0
        desc = None
        alpha = None
        offset = None
        count = None
        storekey = None
        sortby = None
        donsort = False
        getops = []
        N = len(request)
        j = 2
        while j < N:
            val = request[j].lower()
            right = N - j
            if val == b'asc':
                desc = False
            elif val == b'desc':
                desc = True
            elif val == b'alpha':
                alpha = True
            elif val == b'limit' and right >= 2:
                try:
                    offset = int(request[j+1])
                    count = int(request[j+2])
                except Exception:
                    return connection.write(self.SYNTAX_ERROR)
                j += 2
            elif val == b'store' and right >= 1:
                storekey = request[j+1]
                j += 1
            elif val == b'by' and right >= 1:
                sortby = request[j+1]
                if b'*' not in sortby:
                    donsort = True
                j += 1
            elif val == b'get' and right >= 1:
                getops.append(request[j+1])
                j += 1
            else:
                return connection.write(self.SYNTAX_ERROR)
            j += 1

    if sort_type is self.zset_type and dontsort:
        donsort = False
        alpha = True
        sortby = None

    vlen = len(value)
    vector = []

    if not dontsort:
        for val in sorting_value:
            if sortby:
                byval = lookup(sortby, val)
            else:
                byval = val
            if not alpha:
                try:
                    byval = float(byval)
                except Excetion:
                    byval = nan
            vector.append(byval)

    @command('keys', True)
    def ttl(self, connection, request, N):
        check_input(request, N != 1)
        connection.int_reply(connection.db.ttl(request[1]))

    @command('keys', True)
    def type(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            result = 'none'
        else:
            result = self._type_name_map[type(value)]
        connection.write(('+%s\r\n' % result).encode('utf-8'))

    ###########################################################################
    ##    STRING COMMANDS
    @command('strings', True)
    def append(self, connection, request, N):
        check_input(request, N != 2,)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = bytearray(request[2])
            db._data[key] = value
        elif not isinstance(value, bytearray):
            return connection.write(self.WRONG_TYPE)
        else:
            value.extend(request[2])
        self._signal(NOTIFY_STRING, db, request[0], key, 1)
        connection.int_reply(len(value))

    @command('strings', True)
    def decr(self, connection, request, N):
        check_input(request, N != 1)
        r = self._incrby(connection, request[0], request[1], b'-1', int)
        connection.int_reply(r)

    @command('strings', True)
    def decrby(self, connection, request, N):
        check_input(request, N != 2)
        try:
            val = str(-int(request[2])).encode('utf-8')
        except Exception:
            val = request[2]
        r = self._incrby(connection, request[0], request[1], val, int)
        connection.int_reply(r)

    @command('strings')
    def get(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.NIL)
        elif isinstance(value, bytearray):
            connection.write(self._parser.bulk(bytes(value)))
        else:
            connection.write(self.WRONG_TYPE)

    @command('strings')
    def getbit(self, connection, request, N):
        check_input(request, N != 2)
        try:
            offset = int(request[2])
            if offset < 0 or offset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return connection.error_reply("Wrong offset in '%s' command" %
                                          request[0])
        string = connection.db.get(request[1])
        if string is None:
            connection.write(self.ZERO)
        elif not isinstance(string, bytearray):
            connection.write(self.WRONG_TYPE)
        else:
            v = string[offset] if offset < len(string) else 0
            connection.int_reply(v)

    @command('strings')
    def getrange(self, connection, request, N):
        check_input(request, N != 3)
        try:
            start = int(request[2])
            end = int(request[3])
        except Exception:
            return connection.error_reply("Wrong offset in '%s' command" %
                                          request[0])
        string = connection.db.get(request[1])
        if string is None:
            connection.write(self.NIL)
        elif not isinstance(string, bytearray):
            connection.write(self.WRONG_TYPE)
        else:
            if start < 0:
                start = len(string) + start
            if end < 0:
                end = len(string) + end + 1
            else:
                end += 1
            connection.write(self._parser.bulk(bytes(string[start:end])))

    @command('strings', True)
    def getset(self, connection, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            db._data[key] = bytearray(request[2])
            self._signal(NOTIFY_STRING, db, 'set', key, 1)
            connection.write(self.NIL)
        elif isinstance(value, bytearray):
            db.pop(key)
            db._data[key] = bytearray(request[2])
            self._signal(NOTIFY_STRING, db, 'set', key, 1)
            connection.write(self._parser.bulk(bytes(value)))
        else:
            connection.write(self.WRONG_TYPE)

    @command('strings', True)
    def incr(self, connection, request, N):
        check_input(request, N != 1)
        r = self._incrby(connection, request[0], request[1], b'1', int)
        connection.int_reply(r)

    @command('strings', True)
    def incrby(self, connection, request, N):
        check_input(request, N != 2)
        r = self._incrby(connection, request[0], request[1], request[2], int)
        connection.int_reply(r)

    @command('strings', True)
    def incrbyfloat(self, connection, request, N):
        check_input(request, N != 2)
        r = self._incrby(connection, request[0], request[1], request[2], float)
        connection.write(self._parser.bulk(str(r).encode('utf-8')))

    @command('strings')
    def mget(self, connection, request, N):
        check_input(request, not N)
        get = connection.db.get
        values = []
        for key in request[1:]:
            value = get(key)
            if value is None:
                values.append(value)
            elif isinstance(value, bytearray):
                values.append(bytes(value))
            else:
                return connection.write(self.WRONG_TYPE)
        connection.write(self._parser.multi_bulk(*values))

    @command('strings', True)
    def mset(self, connection, request, N):
        D = N // 2
        check_input(request, N < 2 or D * 2 != N)
        db = connection.db
        for key, value in zip(request[1::2], request[2::2]):
            db.pop(key)
            db._data[key] = bytearray(value)
            self._signal(NOTIFY_STRING, db, 'set', key, 1)
        connection.write(self.OK)

    @command('strings', True)
    def msetnx(self, connection, request, N):
        D = N // 2
        check_input(request, N < 2 or D * 2 != N)
        db = connection.db
        keys = request[1::2]
        exist = False
        for key in keys:
            exist = db.exists(key)
            if exist:
                break
        if exist:
            connection.write(self.ZERO)
        else:
            for key, value in zip(keys, request[2::2]):
                db._data[key] = bytearray(value)
                self._signal(NOTIFY_STRING, db, 'set', key, 1)
            connection.write(self.ONE)

    @command('strings', True)
    def psetex(self, connection, request, N):
        check_input(request, N != 3)
        self._set(connection, request[1], request[3], milliseconds=request[2])

    @command('strings', True)
    def set(self, connection, request, N):
        check_input(request, N < 2 or N > 8)
        db = connection.db
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
                    seconds = request[it]
                elif opt == b'px':
                    it += 1
                    milliseconds = request[it]
                elif opt == b'nx':
                    nx = True
                else:
                    xx = True
        self._set(connection, request[1], request[2], seconds,
                  milliseconds, nx, xx)

    @command('strings', True)
    def setbit(self, connection, request, N):
        check_input(request, N != 3)
        key = request[1]
        try:
            offset = int(request[2])
            if offset < 0 or offset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return connection.error_reply("Wrong offset in '%s' command" %
                                          request[0])
        try:
            value = int(request[3])
            if value not in (0, 1):
                raise ValueError
        except Exception:
            return connection.error_reply("Wrong value in '%s' command" %
                                          request[0])
        db = connection.db
        string = db.get(key)
        if string is None:
            string = bytearray(b'\x00')
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return connection.write(self.WRONG_TYPE)
        N = len(string)
        if N < offset:
            string.extend((offset + 1 - N)*b'\x00')
        orig = string[offset]
        string[offset] = value
        self._signal(NOTIFY_STRING, db, request[0], key, 1)
        connection.int_reply(orig)

    @command('strings', True)
    def setex(self, connection, request, N):
        check_input(request, N != 3)
        self._set(connection, request[1], request[3], seconds=request[2])

    @command('strings', True)
    def setnx(self, connection, request, N):
        check_input(request, N != 2)
        self._set(connection, request[1], request[2], nx=True,
                  OK=self.ONE, SKIP=self.ZERO)

    @command('strings', True)
    def setrange(self, connection, request, N):
        check_input(request, N != 3)
        key = request[1]
        value = request[3]
        try:
            offset = int(request[2])
            T = offset + len(value)
            if offset < 0 or T >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return connection.error_reply("Wrong offset in '%s' command" %
                                          request[0])
        db = connection.db
        string = db.get(key)
        if string is None:
            string = bytearray(b'\x00')
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return connection.write(self.WRONG_TYPE)
        N = len(string)
        if N < T:
            string.extend((T + 1 - N)*b'\x00')
        string[offset:T] = value
        self._signal(NOTIFY_STRING, db, request[0], key, 1)
        connection.int_reply(len(string))

    @command('strings')
    def strlen(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif isinstance(value, bytearray):
            connection.int_reply(len(value))
        else:
            return connection.write(self.WRONG_TYPE)

    ###########################################################################
    ##    HASHES COMMANDS
    @command('hashes', True)
    def hdel(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif isinstance(value, self.hash_type):
            rem = 0
            for field in request[2:]:
                rem += 0 if value.pop(field, None) is None else 1
            self._signal(NOTIFY_HASH, db, request[0], key, rem)
            if db.pop(key, value) is not None:
                self._signal(NOTIFY_GENERIC, db, 'del', key)
            connection.int_reply(rem)
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes')
    def hexists(self, connection, request, N):
        check_input(request, N != 2)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif isinstance(value, self.hash_type):
            connection.int_reply(int(request[2] in value))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes')
    def hget(self, connection, request, N):
        check_input(request, N != 2)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.NIL)
        elif isinstance(value, self.hash_type):
            connection.write(self._parser.bulk(value.get(request[2])))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes')
    def hgetall(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif isinstance(value, self.hash_type):
            connection.write(self._parser.multi_bulk(*value.flat()))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes', True)
    def hincrby(self, connection, request, N):
        result = self._hincrby(connection, request, N, int)
        connection.int_reply(result)

    @command('hashes', True)
    def hincrbyfloat(self, connection, request, N):
        result = self._hincrby(connection, request, N, float)
        connection.write(self._parser.bulk(str(result).encode('utf-8')))

    @command('hashes')
    def hkeys(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif isinstance(value, self.hash_type):
            connection.write(self._parser.multi_bulk(*tuple(value)))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes')
    def hlen(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif isinstance(value, self.hash_type):
            connection.int_reply(len(value))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes')
    def hmget(self, connection, request, N):
        check_input(request, N < 3)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif isinstance(value, self.hash_type):
            result = value.mget(request[2:])
            connection.write(self._parser.multi_bulk(result))
        else:
            connection.write(self.WRONG_TYPE)

    @command('hashes', True)
    def hmset(self, connection, request, N):
        D = (N - 1) // 2
        check_input(request, N < 3 or D * 2 != N - 1)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return connection.write(self.WRONG_TYPE)
        value.update(zip(request[2::2], request[3::2]))
        self._signal(NOTIFY_HASH, db, request[0], key, D)
        connection.write(self.OK)

    @command('hashes', True)
    def hset(self, connection, request, N):
        check_input(request, N != 3)
        key, field = request[1], request[2]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return connection.write(self.WRONG_TYPE)
        result = self.ZERO if field in value else self.ONE
        value[field] = request[3]
        self._signal(NOTIFY_HASH, db, request[0], key, 1)
        connection.write(result)

    @command('hashes', True)
    def hsetnx(self, connection, request, N):
        check_input(request, N != 3)
        key, field = request[1], request[2]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return connection.write(self.WRONG_TYPE)
        if field in value:
            connection.write(self.ZERO)
        else:
            value[field] = request[3]
            self._signal(NOTIFY_HASH, db, request[0], key, 1)
            connection.write(self.ONE)

    @command('hashes')
    def hvals(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif isinstance(value, self.hash_type):
            connection.write(self._parser.multi_bulk(*tuple(value.values())))
        else:
            connection.write(self.WRONG_TYPE)

    ###########################################################################
    ##    LIST COMMANDS
    @command('lists', True)
    def blpop(self, connection, request, N):
        check_input(request, N < 2)
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._bpop(connection, request, keys):
            self.wait_for_keys(connection, timeout, keys)

    @command('lists', True)
    def brpop(self, connection, request, N):
        return self.blpop(connection, request, N)

    @command('lists', True)
    def brpoplpush(self, connection, request, N):
        check_input(request, N < 2)
        timeout = max(0, int(request[-1]))
        keys = request[1:-1]
        if not self._bpop(connection, request, keys):
            self.wait_for_keys(connection, timeout, keys)

    @command('lists')
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

    @command('lists', True)
    def linsert(self, connection, request, N):
        # This method is INEFFICIENT, but redis supported so we do
        # the same here
        check_input(request, N != 4)
        db = connection.db
        key = request[1]
        value = db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            where = request[2].lower()
            pivot = request[3]
            if where == b'before':
                value.insert_before(request[3], request[4])
            elif where == b'after':
                value.insert_after(request[3], request[4])
            else:
                return connection.error_reply('cannot insert to list')
            self._signal(NOTIFY_LIST, db, request[0], key, 1)
            connection.int_reply(len(value))

    @command('lists')
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

    @command('lists', True)
    def lpop(self, connection, request, N):
        check_input(request, N != 1)
        db = connection.db
        key = request[1]
        value = db.get(key)
        if value is None:
            connection.write(self.NIL)
        elif isinstance(value, self.list_type):
            if request[1] == 'lpop':
                result = value.popleft()
            else:
                result = value.pop()
            self._signal(NOTIFY_LIST, db, request[0], key, 1)
            if db.pop(key, value) is not None:
                self._signal(NOTIFY_GENERIC, db, 'del', key)
            connection.write(self._parser.bulk(result))
        else:
            connection.write(self.WRONG_TYPE)

    @command('lists', True)
    def rpop(self, connection, request, N):
        return self.lpop(connection, request, N)

    @command('lists', True)
    def lpush(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.list_type()
            db._data[key] = value
        elif not isinstance(value, self.list_type):
            return connection.write(self.WRONG_TYPE)
        if request[0] == 'lpush':
            value.extendleft(request[2:])
        else:
            value.extend(request[2:])
        connection.int_reply(len(value))
        self._signal(NOTIFY_LIST, db, request[0], key, N - 1)

    @command('lists', True)
    def rpush(self, connection, request, N):
        return self.lpush(connection, request, N)

    @command('lists', True)
    def lpushx(self, connection, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            if request[0] == 'lpush':
                value.appendleft(request[2])
            else:
                value.append(request[2])
            connection.int_reply(len(value))
            self._signal(NOTIFY_LIST, db, request[0], key, 1)
    rpushx = lpushx

    @command('lists', True)
    def lrange(self, connection, request, N):
        check_input(request, N != 3)
        db = connection.db
        key = request[1]
        value = db.get(key)
        try:
            start, end = self._range_values(value, request[2], request[3])
        except Exception:
            return connection.error_reply('invalid range')
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            elems = islice(value, start, end)
            chunk = self._parser.multi_bulk(*tuple(elems))
            connection.write(chunk)

    @command('lists', True)
    def lrem(self, connection, request, N):
        # This method is INEFFICIENT, but redis supported so we do
        # the same here
        check_input(request, N != 3)
        db = connection.db
        key = request[1]
        value = db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            try:
                count = int(request[2])
            except Exception:
                return connection.error_reply('cannot remove from list')
            removed = value.remove(request[3], count)
            if removed:
                self._signal(NOTIFY_LIST, db, request[0], key, removed)
            connection.int_reply(removed)

    @command('lists', True)
    def lset(self, connection, request, N):
        check_input(request, N != 3)
        db = connection.db
        key = request[1]
        value = db.get(key)
        if value is None:
            connection.write(self.OUT_OF_BOUND)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            try:
                index = int(request[2])
            except Exception:
                index = -1
            if index >= 0 and index < len(value):
                value[index] = request[3]
                self._signal(NOTIFY_LIST, db, request[0], key, 1)
                connection.write(self.OK)
            else:
                connection.write(self.OUT_OF_BOUND)

    @command('lists', True)
    def ltrim(self, connection, request, N):
        check_input(request, N != 3)
        db = connection.db
        key = request[1]
        value = db.get(key)
        try:
            start, end = self._range_values(value, request[2], request[3])
        except Exception:
            return connection.error_reply('invalid range')
        if value is None:
            connection.write(self.OK)
        elif not isinstance(value, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            start = len(value)
            value.trim(start, end)
            connection.write(self.OK)
            self._signal(NOTIFY_LIST, db, request[0], key, start-len(value))
            connection.write(self.OK)

    @command('lists', True)
    def rpoplpush(self, connection, request, N):
        check_input(request, N != 2)
        key1, key2 = request[1], request[2]
        db = connection.db
        orig = db.get(key1)
        dest = db.get(key2)
        if orig is None:
            connection.write(self.NIL)
        elif not isinstance(orig, self.list_type):
            connection.write(self.WRONG_TYPE)
        else:
            if dest is None:
                dest = self.list_type()
                db._data[key2] = dest
            elif not isinstance(dest, self.list_type):
                return connection.write(self.WRONG_TYPE)
            value = orig.pop()
            self._signal(NOTIFY_LIST, db, 'rpop', key1, 1)
            dest.appendleft(value)
            self._signal(NOTIFY_LIST, db, 'lpush', key2, 1)
            if db.pop(key1, orig) is not None:
                self._signal(NOTIFY_GENERIC, db, 'del', key1)
            connection.write(self._parser.bulk(value))

    ###########################################################################
    ##    SETS COMMANDS
    @command('sets', True)
    def sadd(self, connection, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = set()
            db._data[key] = value
        elif not isinstance(value, set):
            return connection.write(self.WRONG_TYPE)
        n = len(value)
        value.update(request[2:])
        n = len(value) - n
        self._signal(NOTIFY_SET, db, request[0], key, n)
        connection.int_reply(n)

    @command('sets')
    def scard(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.int_reply(len(value))

    @command('sets')
    def sdiff(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'difference', request[1:])

    @command('sets', True)
    def sdiffstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'difference', request[2:], request[1])

    @command('sets')
    def sinter(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'intersection', request[1:])

    @command('sets', True)
    def sinterstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'intersection', request[2:], request[1])

    @command('sets')
    def sismemeber(self, connection, request, N):
        check_input(request, N != 2)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.int_reply(int(request[2] in value))

    @command('sets')
    def smembers(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.EMPTY_ARRAY)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            connection.write(self._parser.multi_bulk(*value))

    @command('sets', True)
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
                self._signal(NOTIFY_SET, db, 'srem', key1)
                self._signal(NOTIFY_SET, db, 'sadd', key2, 1)
                if db.pop(key1, orig) is not None:
                    self._signal(NOTIFY_GENERIC, db, 'del', key1)
                connection.write(self.ONE)
            else:
                connection.write(self.ZERO)

    @command('sets', True)
    def spop(self, connection, request, N):
        check_input(request, N != 1)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            connection.write(self.NIL)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            result = value.pop()
            self._signal(NOTIFY_SET, db, request[0], key, 1)
            if db.pop(key, value) is not None:
                self._signal(NOTIFY_GENERIC, db, 'del', key)
            connection.write(self._parser.bulk(result))

    @command('sets')
    def srandmember(self, connection, request, N):
        check_input(request, N < 1 or N > 2)
        value = connection.db.get(request[1])
        if value is not None and not isinstance(value, set):
            return connection.write(self.WRONG_TYPE)
        if N == 2:
            try:
                count = int(request[2])
            except Exception:
                return connection.error_reply('Invalid count')
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
            connection.write(self._parser.multi_bulk(*result))
        else:
            if not value:
                result = None
            else:
                result = value.pop()
                value.add(result)
            connection.write(self._parser.bulk(result))

    @command('sets', True)
    def srem(self, connection, request, N):
        check_input(request, N < 2)
        db = connection.db
        key = request[1]
        value = db.get(key)
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, set):
            connection.write(self.WRONG_TYPE)
        else:
            start = len(value)
            value.difference_update(request[2:])
            removed = start - len(value)
            self._signal(NOTIFY_SET, db, request[0], key, removed)
            if db.pop(key, value) is not None:
                self._signal(NOTIFY_GENERIC, db, 'del', key)
            connection.int_reply(removed)

    @command('sets')
    def sunion(self, connection, request, N):
        check_input(request, N < 1)
        self._setoper(connection, 'union', request[1:])

    @command('sets', True)
    def sunionstore(self, connection, request, N):
        check_input(request, N < 2)
        self._setoper(connection, 'union', request[2:], request[1])

    ###########################################################################
    ##    SORTED SETS COMMANDS
    @command('Sorted sets', True)
    def zadd(self, connection, request, N):
        D = (N - 1) // 2
        check_input(request, N < 3 or D * 2 != N - 1)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            value = self.zset_type()
            db._data[key] = value
        elif not isinstance(value, self.zset_type):
            return connection.write(self.WRONG_TYPE)
        start = len(value)
        value.update(zip(map(float, request[2::2]), request[3::2]))
        result = len(value) - start
        self._signal(NOTIFY_ZSET, db, request[0], key, result)
        connection.int_reply(result)

    @command('Sorted sets')
    def zcard(self, connection, request, N):
        check_input(request, N != 1)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, self.zset_type):
            connection.write(self.WRONG_TYPE)
        else:
            connection.int_reply(len(value))

    @command('Sorted sets')
    def zcount(self, connection, request, N):
        check_input(request, N != 3)
        value = connection.db.get(request[1])
        if value is None:
            connection.write(self.ZERO)
        elif not isinstance(value, self.zset_type):
            connection.write(self.WRONG_TYPE)
        else:
            try:
                mmin = float(request[2])
                mmax = float(request[3])
            except Exception:
                connection.write(self.INVALID_SCORE)
            else:
                connection.int_reply(value.count(mmin, mmax))

    @command('Sorted sets', True)
    def zincrby(self, connection, request, N):
        check_input(request, N != 3)
        key = request[1]
        db = connection.db
        value = db.get(key)
        if value is None:
            db._data[key] = value = self.zset_type()
        elif not isinstance(value, self.zset_type):
            return connection.write(self.WRONG_TYPE)
        try:
            increment = float(request[2])
        except Exception:
            connection.write(self.INVALID_SCORE)
        else:
            member = request[3]
            score = value.score(member, 0) + increment
            value.add(score, member)
            self._signal(NOTIFY_ZSET, db, request[0], key, 1)
            connection.write(self._parser.bulk(str(score).encode('utf-8')))


    ###########################################################################
    ##    PUBSUB COMMANDS
    @command('Pub/Sub')
    def psubscribe(self, connection, request, N):
        check_input(request, not N)
        for pattern in request[1:]:
            p = self._patterns.get(pattern)
            if not p:
                p = pubsub_patterns(re.compile(pattern.decode('utf-8')), set())
                self._patterns[pattern] = p
            p.clients.add(connection)
            connection.patterns.add(pattern)
            count = reduce(lambda x, y: x + int(connection in y.clients),
                           self._patterns.values())
            chunk = self._parser.multi_bulk(b'psubscribe', pattern, count)
            connection.write(chunk)

    @command('Pub/Sub')
    def pubsub(self, connection, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'channels':
            if N > 2:
                check_input(request, False)
            elif N == 2:
                pre = re.compile(request[2].decode('utf-8', errors='ignore'))
                channels = []
                for channel in self._channels:
                    if pre.match(channel.decode('utf-8', errors='ignore')):
                        channels.append(channel)
            else:
                channels = list(self._channels)
            connection.write(self._parser.multi_bulk(*channels))
        elif subcommand == 'numsub':
            check_input(request, N > 1)
            count = []
            for channel in request[2:]:
                clients = self._channels.get(channel, ())
                count.append(en(clients))
            connection.write(self._parser.multi_bulk(*count))
        elif subcommand == 'numpat':
            check_input(request, N == 1)
            count = reduce(lambda x, y: x + len(y.clients),
                           self._patterns.values())
            connection.int_reply(count)
        else:
            raise CommandError("unknown command 'pubsub %s'" % subcommand)

    @command('Pub/Sub')
    def publish(self, connection, request, N):
        check_input(request, N != 2)
        channel, message = request[1:]
        ch = channel.decode('utf-8')
        clients = self._channels.get(channel)
        msg = self._parser.multi_bulk(b'message', channel, message)
        count = self._publish_clients(msg, self._channels.get(channel, ()))
        for pattern in self._patterns.values():
            g = pattern.re.match(ch)
            if g:
                count += self._publish_clients(msg, pattern.clients)
        connection.int_reply(count)

    @command('Pub/Sub')
    def punsubscribe(self, connection, request, N):
        check_input(request, not N)
        patterns = list(self._patterns) if N==1 else request[1:]
        for pattern in patterns:
            if pattern in self._patterns:
                p = self._patterns[pattern]
                if connection in p.clients:
                    connection.patterns.discard(pattern)
                    p.clients.remove(connection)
                    if not p.clients:
                        self._patterns.pop(pattern)
                    chunk = self._parser.multi_bulk(b'punsubscribe', pattern)
                    connection.write(chunk)

    @command('Pub/Sub')
    def subscribe(self, connection, request, N):
        check_input(request, not N)
        for channel in request[1:]:
            clients = self._channels.get(channel)
            if not clients:
                self._channels[channel] = clients = set()
            clients.add(connection)
            connection.channels.add(channel)
            chunk = self._parser.multi_bulk(b'subscribe', channel,
                                            len(clients))
            connection.write(chunk)

    @command('Pub/Sub')
    def unsubscribe(self, connection, request, N):
        check_input(request, not N)
        channels = list(self._channels) if N==1 else request[1:]
        for channel in channels:
            if channel in self._channels:
                clients = self._channels[channel]
                if connection in clients:
                    connection.channels.discard(channel)
                    clients.remove(connection)
                    if not clients:
                        self._channels.pop(channel)
                    chunk = self._parser.multi_bulk(b'unsubscribe', channel)
                    connection.write(chunk)

    ###########################################################################
    ##    TRANSACTION COMMANDS
    @command('transactions')
    def discard(self, connection, request, N):
        check_input(request, N)
        if connection.transaction is None:
            connection.error_reply("DISCARD without MULTI")
        else:
            self._close_transaction(connection)
            connection.write(self.OK)

    @command('transactions', name='exec')
    def execute(self, connection, request, N):
        check_input(request, N)
        if connection.transaction is None:
            connection.error_reply("EXEC without MULTI")
        else:
            requests = connection.transaction
            if connection.flag & DIRTY_CAS:
                self._close_transaction(connection)
                connection.write(self.EMPTY_ARRAY)
            else:
                connection.transaction = []
                for handle, request in requests:
                    connection.execute(handle, request)
                result = connection.transaction
                self._close_transaction(connection)
                connection.write(self._parser.multi_bulk(*result))

    @command('transactions')
    def multi(self, connection, request, N):
        check_input(request, N)
        if connection.transaction is None:
            connection.write(self.OK)
            connection.transaction = []
        else:
            self.error_replay("MULTI calls can not be nested")

    @command('transactions')
    def watch(self, connection, request, N):
        check_input(request, not N)
        if connection.transaction is not None:
            connection.error_reply("WATCH inside MULTI is not allowed")
        else:
            wkeys = connection.watched_keys
            if not wkeys:
                connection.watched_keys = wkeys = set()
                self._watching.add(connection)
            wkeys.update(request[1:])
            connection.write(self.OK)

    @command('transactions')
    def unwatch(self, connection, request, N):
        check_input(request, N)
        transaction = connection.transaction
        self._close_transaction(connection)
        connection.transaction = transaction
        connection.write(self.OK)

    ###########################################################################
    ##    CONNECTION COMMANDS
    @command('connection')
    def auth(self, connection, request, N):
        check_input(request, N != 1)
        conn.password = request[1]
        if conn.password != conn._producer.password:
            connection.error_reply("wrong password")
        else:
            connection.write(self.OK)

    @command('connection')
    def echo(self, connection, request, N):
        check_input(request, N != 1)
        connection.write(self._parser.bulk(request[1]))

    @command('connection')
    def ping(self, connection, request, N):
        check_input(request, N)
        connection.write(self.PONG)

    @command('connection')
    def quit(self, connection, request, N):
        check_input(request, N)
        connection.write(self.OK)
        connection.close()

    @command('connection')
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
    @command('server')
    def bgsave(self, connection, request, N):
        check_input(request, N)
        self._save()
        connection.write(self.OK)

    @command('server')
    def client(self, connection, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'list':
            check_input(request, N != 1)
            value = '\n'.join(self._client_list(connection))
            connection.write(self._parser.bulk(value.encode('utf-8')))
        else:
            raise CommandError("unknown command 'client %s'" % subcommand)

    @command('server')
    def config(self, connection, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'get':
            if N != 2:
                connection.error_reply("'config get' no argument")
            else:
                value = self._get_config(request[2].decode('utf-8'))
                connection.write(self._parser.bulk(value))
        elif subcommand == 'rewrite':
            connection.write(self.OK)
        elif subcommand  == 'set':
            try:
                if N != 3:
                    raise ValueError("'config set' no argument")
                self._set_config(request[2].decode('utf-8'))
            except Exception as e:
                connection.error_reply(str(e))
            else:
                connection.write(self.OK)
        elif subcommand == 'resetstat':
            self._hit_keys = 0
            self._missed_keys = 0
            self._expired_keys = 0
            server = connection._producer
            server._received = 0
            server._requests_processed = 0
            connection.write(self.OK)
        else:
            connection.error_reply("'config %s' not valid" % subcommand)

    @command('server')
    def dbsize(self, connection, request, N):
        check_input(request, N != 0)
        connection.int_reply(len(connection.db))

    @command('server', True)
    def flushdb(self, connection, request, N):
        check_input(request, N)
        connection.db.flush()
        connection.write(self.OK)

    @command('server', True)
    def flushall(self, connection, request, N):
        check_input(request, N)
        for db in self.databases.values():
            db.flush()
        connection.write(self.OK)

    @command('server')
    def info(self, connection, request, N):
        check_input(request, N)
        info = '\n'.join(self._flat_info())
        chunk = self._parser.bulk(info.encode('utf-8'))
        connection.write(chunk)

    @command('server')
    def monitor(self, connection, request, N):
        check_input(request, N)
        connection.flag |= MONITOR
        self._monitors.add(connection)
        connection.write(self.OK)

    @command('server')
    def save(self, connection, request, N):
        check_input(request, N)
        self._save(False)
        connection.write(self.OK)

    @command('server')
    def time(self, connection, request, N):
        check_input(request, N != 0)
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        chunk = self._parser.multi_bulk(seconds, microseconds)
        connection.write(chunk)

    ###########################################################################
    ##    INTERNALS
    def wait_for_keys(self, connection, timeout, keys):
        db = connection.db
        connection.blocked = b = Blocked(connection, timeout)
        for key in keys:
            if key not in b.keys:
                clients = db._blocking_keys.get(key)
                if clients is None:
                    db._blocking_keys[key] = clients = []
                clients.append(connection)
        self._bpop_blocked_clients += 1

    def _cron(self):
        dirty = self._dirty
        if dirty:
            now = time.time()
            gap = now - self._last_save
            for interval, changes in self.cfg.key_value_save:
                if gap >= interval and dirty >= changes:
                    self._save()
                    break

    def _set(self, connection, key, value,
             seconds=0, milliseconds=0, nx=False, xx=False,
             OK=None, SKIP=None):
        try:
            seconds = int(seconds)
            milliseconds = 0.000001*int(milliseconds)
            if seconds < 0 or milliseconds < 0:
                raise ValueError
        except Exception:
            return connection.error_reply('invalid expire time')
        else:
            timeout = seconds + milliseconds
        db = connection.db
        exists = db.exists(key)
        skip = (exists and nx) or (not exists and xx)
        if not skip:
            if exists:
                db.pop(key)
            if timeout > 0:
                db._expires[key] = (self._loop.call_later(
                    timeout, db._do_expire, key), bytearray(value))
                self._signal(NOTIFY_STRING, db, 'expire', key)
            else:
                db._data[key] = bytearray(value)
            self._signal(NOTIFY_STRING, db, 'set', key, 1)
            connection.write(OK or self.OK)
        else:
            connection.write(SKIP or self.NULL_ARRAY)

    def _incrby(self, connection, name, key, value, type):
        try:
            tv = type(value)
        except Exception:
            return connection.error_reply('invalid increment')
        db = connection.db
        cur = db.get(key)
        if cur is None:
            db._data[key] = bytearray(value)
        elif isinstance(cur, bytearray):
            try:
                tv += type(cur)
            except Exception:
                return connection.error_reply('invalid increment')
            db._data[key] = bytearray(str(tv).encode('utf-8'))
        else:
            return connection.write(self.WRONG_TYPE)
        self._signal(NOTIFY_STRING, db, name, key, 1)
        return tv

    def _bpop(self, connection, request, keys):
        list_type = self.list_type
        db = connection.db
        name = request[0][1:]
        method = 'popleft' if name == 'lpop' else 'pop'
        for key in keys:
            value = db.get(key)
            if isinstance(value, list_type):
                result = (key, getattr(value, method)())
                self._signal(NOTIFY_LIST, db, name, key, 1)
                if db.pop(key, value) is not None:
                    self._signal(NOTIFY_GENERIC, db, 'del', key)
                connection.write(self._parser.multi_bulk(result))
                return True
            elif value is not None:
                connection.write(self.WRONG_TYPE)
                return True
        return False

    def _range_values(self, value, start, end):
        start = int(start)
        end = int(end)
        if value is not None:
            if start < 0:
                start = len(value) + start
            if end < 0:
                end = len(value) + end + 1
            else:
                end += 1
        return start, end

    def _close_transaction(self, connection):
        connection.transaction = None
        connection.watched_keys = None
        connection.flag &= ~DIRTY_CAS
        self._watching.discard(connection)

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

    def _get_config(self, name):
        return b''

    def _set_config(self, name, value):
        pass

    def _encode_info_value(self, value):
        return str(value).replace('=',
                                  ' ').replace(',',
                                               ' ').replace('\n', ' - ')

    def _hincrby(self, connection, request, N, type):
        check_input(request, N != 3)
        key, field, increment = request[1], request[2], type(request[3])
        db = connection.db
        hash = db.get(key)
        if hash is None:
            db._data[key] = self.hash_type(field=increment)
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
            db.pop(dest)
            if result:
                db._data[dest] = result
                connection.int_reply(len(result))
            else:
                connection.write(self.ZERO)
        else:
            connection.write(self._parser.multi_bulk(*result))

    def _info(self):
        keyspace = {}
        stats = {'keyspace_hits': self._hit_keys,
                 'keyspace_misses': self._missed_keys,
                 'expired_keys': self._expired_keys,
                 'keys_changed': self._dirty,
                 'pubsub_channels': len(self._channels),
                 'pubsub_patterns': len(self._patterns),
                 'blocked_clients': self._bpop_blocked_clients}
        persistance = {'rdb_changes_since_last_save': self._dirty,
                       'rdb_last_save_time': self._last_save}
        for db in self.databases.values():
            if len(db):
                keyspace[str(db)] = db.info()
        return {'keyspace': keyspace,
                'stats': stats,
                'persistance': persistance}

    def _client_list(self, connection):
        for client in connection._producer._concurrent_connections:
            yield ' '.join(self._client_info(client))

    def _client_info(self, client):
        yield 'addr=%s:%s' % client._transport.get_extra_info('addr')
        yield 'fd=%s' % client._transport._sock_fd
        yield 'age=%s' % int(time.time() - client.started)
        yield 'db=%s' % client.database
        yield 'sub=%s' % len(client.channels)
        yield 'psub=%s' % len(client.patterns)
        yield 'cmd=%s' % client.last_command

    def _save(self, async=True):
        self._dirty = 0
        self._last_save = int(time.time())
        data = StorageData(self)
        data.save(self._filename, async)

    def _loaddb(self):
        filename = self._filename
        if os.path.isfile(filename):
            with open(filename, 'rb') as file:
                data = pickle.load(file)
            data.load(self)

    def _signal(self, type, db, command, key=None, dirty=0):
        self._dirty += dirty
        self._event_handlers[type](db, key, COMMANDS_INFO[command])

    def _publish_clients(self, msg, clients):
        remove = set()
        count = 0
        for client in clients:
            try:
                client.write(msg)
                count += 1
            except Exception:
                remove.add(client)
        if remove:
            clients.difference_update(remove)
        return count

    # EVENT HANDLERS
    def _modified_key(self, key):
        for client in self._watching:
            if key is None or key in client.watched_keys:
                client.flag |= DIRTY_CAS

    def _generic_event(self, db, key, command):
        if command.write:
            self._modified_key(key)

    _string_event = _generic_event
    _set_event = _generic_event
    _hash_event = _generic_event
    _zset_event = _generic_event

    def _list_event(self, db, key, command):
        if command.write:
            self._modified_key(key)
        if key in db._blocking_keys:
            if command in self._unblock_commands:
                for client in db._blocking_keys.pop(key):
                    client.blocked.callback()

    def _remove_connection(self, client, _):
        self._monitors.discard(client)
        self._watching.discard(client)
        for channel, clients in list(self._channels.items()):
            clients.discard(client)
            if not clients:
                self._channels.pop(channel)
        for pattern, p in list(self._patterns.items()):
            p.clients.discard(client)
            if not p.clients:
                self._patterns.pop(pattern)
        return _

    def _write_to_monitors(self, connection, request):
        now = time.time
        addr = '%s:%s' % self._transport.get_extra_info('addr')
        cmds = b'" "'.join(request)
        message = '+%s [0 %s] "'.encode(utf-8) + cmds + b'"\r\n'
        remove = set()
        for m in self._monitors:
            try:
                m._transport.write(message)
            except Exception:
                remove.add(m)
        if remove:
            self._monitors.difference_update(remove)


class Db(object):
    '''The database.
    '''
    def __init__(self, num, storage):
        self._num = num
        self._storage = storage
        self._loop = storage._loop
        self._data = {}
        self._expires = {}
        self._events = {}
        self._blocking_keys = {}

    def __repr__(self):
        return 'db%s' % self._num
    __str__ = __repr__

    def __len__(self):
        return len(self._data) + len(self._expires)

    def __iter__(self):
        return chain(self._data, self._expires)

    ###########################################################################
    ##    INTERNALS
    def flush(self):
        removed = len(self._data)
        self._data.clear()
        [handle.cancel() for handle, _ in self._expires.values()]
        self._expires.clear()
        self._storage._signal(NOTIFY_GENERIC, self, 'flushdb', dirty=removed)

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
            timeout, self._do_expire, key), value)
        return True

    def expireat(self, key, timeout):
        if key in self._expires:
            handle, value = self._expires.pop(key)
            handle.cancel()
        elif key in self._data:
            value = self._data.pop(key)
        else:
            return False
        self._expires[key] = (self._loop.call_later(timeout - time.time(),
            timeout, self._do_expire, key), value)
        return True

    def persist(self, key):
        if key in self._expires:
            self._storage._hit_keys += 1
            handle, value = self._expires.pop(key)
            handle.cancel()
            self._data[key] = value
            return True
        elif key in self._data:
            self._storage._hit_keys += 1
        else:
            self._storage._missed_keys += 1
        return False

    def ttl(self, key):
        if key in self._expires:
            self._storage._hit_keys += 1
            handle, value = self._expires[key]
            return max(0, int(handle._when - self._loop.time()))
        elif key in self._data:
            self._storage._hit_keys += 1
            return -1
        else:
            self._storage._missed_keys += 1
            return -2

    def info(self):
        return {'keys': len(self._data),
                'expires': len(self._expires)}

    def pop(self, key, value=None):
        if not value:
            if key in self._data:
                value = self._data.pop(key)
                return value
            elif key in self._expires:
                handle, value, self._expires.pop(key)
                handle.cancel()
                return value

    def rem(self, key):
        if key in self._data:
            self._storage._hit_keys += 1
            self._data.pop(key)
            self._storage._signal(NOTIFY_GENERIC, self, 'del', key, 1)
            return 1
        elif key in self._expires:
            self._storage._hit_keys += 1
            handle, _, self._expires.pop(key)
            handle.cancel()
            self._storage._signal(NOTIFY_GENERIC, self, 'del', key, 1)
            return 1
        else:
            self._storage._missed_keys += 1
            return 0

    def _do_expire(self, key):
        if key in self._expires:
            handle, value, = self._expires.pop(key)
            handle.cancel()
            self._storage._expired_keys += 1


class StorageData:

    def __init__(self, storage):
        self.version = 1
        self.dbs = [(db._num, db._data) for db in storage.databases.values()
                    if len(db._data)]

    def save(self, filename, async=True):
        with open(filename, 'wb') as file:
            pickle.dump(self, file, protocol=2)

    def load(self, storage):
        for num, data in self.dbs:
            db = storage.databases.get(num)
            if db is not None:
                db._data = data
