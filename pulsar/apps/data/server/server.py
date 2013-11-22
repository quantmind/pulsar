'''Classes for the pulsar Key-Value store server
'''
import os
import re
import time
import math
from random import choice
from hashlib import sha1
from itertools import islice, chain
from functools import partial, reduce
from collections import namedtuple

import pulsar
from pulsar.apps.socket import SocketServer
from pulsar.utils.config import Global
from pulsar.utils.structures import Dict, Zset, Deque
from pulsar.utils.pep import map, range, zip, pickle
try:
    from pulsar.utils.lua import Lua
except ImportError:
    Lua = None


from .parser import redis_parser
from .sort import sort_command
from .client import (command, PulsarStoreClient, LuaClient, Blocked,
                     COMMANDS_INFO, check_input, redis_to_py_pattern)


DEFAULT_PULSAR_STORE_ADDRESS = '127.0.0.1:6410'

# Keyspace changes notification classes
STRING_LIMIT = 2**32

nan = float('nan')


class RedisParserSetting(Global):
    name = "py_redis_parser"
    flags = ["--py-redis-parser"]
    action = "store_true"
    default = False
    desc = 'Use the python redis parser rather the C++ implementation.'

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
    type = int
    default = 16
    desc = 'Number of databases for the key value store.'


class KeyValuePassword(KeyValuePairSetting):
    name = "key_value_password"
    flags = ["--key-value-password"]
    default = ''
    desc = 'Optional password for the database.'


class KeyValueSave(KeyValuePairSetting):
    name = "key_value_save"
    default = [(900, 1), (300, 10), (60, 10000)]
    validator = validate_list_of_pairs
    desc = ''''
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
    default = 'pulsarkv.rdb'
    desc = 'The filename where to dump the DB.'


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
        # The set of clients which are watching keys
        self._watching = set()
        # The set of clients which issued the monitor command
        self._monitors = set()
        #
        self.NOTIFY_KEYSPACE = (1 << 0)
        self.NOTIFY_KEYEVENT = (1 << 1)
        self.NOTIFY_GENERIC = (1 << 2)
        self.NOTIFY_STRING = (1 << 3)
        self.NOTIFY_LIST = (1 << 4)
        self.NOTIFY_SET = (1 << 5)
        self.NOTIFY_HASH = (1 << 6)
        self.NOTIFY_ZSET = (1 << 7)
        self.NOTIFY_EXPIRED = (1 << 8)
        self.NOTIFY_EVICTED = (1 << 9)
        self.NOTIFY_ALL = (self.NOTIFY_GENERIC | self.NOTIFY_STRING |
                           self.NOTIFY_LIST | self.NOTIFY_SET |
                           self.NOTIFY_HASH | self.NOTIFY_ZSET |
                           self.NOTIFY_EXPIRED | self.NOTIFY_EVICTED)

        self.MONITOR = (1 << 2)
        self.MULTI = (1 << 3)
        self.BLOCKED = (1 << 4)
        self.DIRTY_CAS = (1 << 5)
        #
        self._event_handlers = {self.NOTIFY_GENERIC: self._generic_event,
                                self.NOTIFY_STRING: self._string_event,
                                self.NOTIFY_SET: self._set_event,
                                self.NOTIFY_HASH: self._hash_event,
                                self.NOTIFY_LIST: self._list_event,
                                self.NOTIFY_ZSET: self._zset_event}
        self._set_options = (b'ex', b'px', 'nx', b'xx')
        self.OK = b'+OK\r\n'
        self.QUEUED = b'+QUEUED\r\n'
        self.ZERO = b':0\r\n'
        self.ONE = b':1\r\n'
        self.NIL = b'$-1\r\n'
        self.NULL_ARRAY = b'*-1\r\n'
        self.INVALID_TIMEOUT = 'invalid expire time'
        self.PUBSUB_ONLY = ('only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT '
                            'allowed in this context')
        self.INVALID_SCORE = 'Invalid score value'
        self.OUT_OF_BOUND = 'Out of bound'
        self.SYNTAX_ERROR = 'Syntax error'
        self.SUBSCRIBE_COMMANDS = ('psubscribe', 'punsubscribe', 'subscribe',
                                   'unsubscribe', 'quit')
        self.hash_type = Dict
        self.list_type = Deque
        self.zset_type = Zset
        self._type_event_map = {bytearray: self.NOTIFY_STRING,
                                self.hash_type: self.NOTIFY_HASH,
                                self.list_type: self.NOTIFY_LIST,
                                set: self.NOTIFY_SET,
                                self.zset_type: self.NOTIFY_ZSET}
        self._type_name_map = {bytearray: 'string',
                               self.hash_type: 'hash',
                               self.list_type: 'list',
                               set: 'set',
                               self.zset_type: 'zset'}
        self.databases = dict(((num, Db(num, self))
                               for num in range(cfg.key_value_databases)))
        # Initialise lua
        if Lua:
            self.lua = Lua()
            self.scripts = {}
            self.lua.register('redis', LuaClient(self),
                              'call', 'pcall', 'error_reply', 'status_reply')
            self.version = '2.6.16'
        else:
            self.lua = None
            self.version = '2.4.10'
        self._loaddb()
        self._loop.call_repeatedly(1, self._cron)

    ###########################################################################
    ##    KEYS COMMANDS
    @command('keys', True, name='del')
    def delete(self, client, request, N):
        check_input(request, not N)
        rem = client.db.rem
        result = reduce(lambda x, y: x + rem(y), request[1:], 0)
        client.int_reply(result)

    @command('keys')
    def exists(self, client, request, N):
        check_input(request, N != 1)
        if client.db.exists(request[1]):
            client.reply_one()
        else:
            client.reply_zero()

    @command('keys', True)
    def expire(self, client, request, N):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            client.reply_error(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return client.reply_error(self.INVALID_TIMEOUT)
                if client.db.expire(request[1], timeout):
                    return client.reply_one()
            client.reply_zero()

    @command('keys', True)
    def expireat(self, client, request, N):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            client.reply_error(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return client.reply_error(self.INVALID_TIMEOUT)
                if client.db.expireat(request[1], timeout):
                    return client.reply_one()
            client.reply_zero()

    @command('keys')
    def keys(self, client, request, N):
        err = 'ignore'
        check_input(request, N != 1)
        pattern = request[1].decode('utf-8', err)
        allkeys = pattern == '*'
        gr = None
        if not allkeys:
            gr = re.compile(redis_to_py_pattern(pattern))
        result = [key for key in client.db if allkeys or
                  gr.search(key.decode('utf-8', err))]
        client.reply_multibulk(result)

    @command('keys', True)
    def move(self, client, request, N):
        check_input(request, N != 2)
        key = request[1]
        try:
            db2 = self.databases.get(int(request[2]))
            if not db:
                raise ValueError
        except Exception:
            return client.reply_error('Invalid database')
        db = client.db
        value = db.get(key)
        if db2.exists(key) or value is None:
            return client.reply_zero()
        db.pop(key)
        self._signal(self.NOTIFY_GENERIC, db, 'del', key, 1)
        db2._data[key] = value
        self._signal(self._type_event_map[type(value)], db2, 'set', key, 1)
        client.reply_one()

    @command('keys', True)
    def persist(self, client, request, N):
        check_input(request, N != 1)
        if client.db.persist(request[1]):
            client.reply_one()
        else:
            client.reply_zero()

    @command('keys')
    def randomkey(self, client, request, N):
        check_input(request, N)
        keys = list(client.db)
        if keys:
            client.reply_bulk(choice(keys))
        else:
            client.reply_bulk()

    @command('keys', True)
    def rename(self, client, request, N, ex=False):
        check_input(request, N != 2)
        key1, key2 = request[1], request[2]
        db = client.db
        value = db.get(key1)
        if value is None:
            client.reply_error('Cannot rename key, not available')
        elif key1 == key2:
            client.reply_error('Cannot rename key')
        else:
            if ex:
                if db.exists(key2):
                    return client.reply_zero()
                result = 1
            else:
                result = 0
                db.rem(key2)
            db.pop(key1)
            event = self._type_event_map[type(value)]
            dirty = 1 if event == self.NOTIFY_STRING else len(value)
            db._data[key2] = value
            self._signal(event, db, request[0], key2, dirty)
            client.reply_one() if result else client.reply_ok()

    @command('keys', True)
    def renameex(self, client, request, N):
        self.rename(client, request, N, True)

    @command('sort', True)
    def sort(self, client, request, N):
        check_input(request, not N)
        value = client.db.get(request[1])
        if value is None:
            value = self.list_type()
        elif not isinstance(value, (set, self.list_type, self.zset_type)):
            return client.reply_wrongtype()
        sort_command(self, client, request, value)

    @command('keys', True)
    def ttl(self, client, request, N):
        check_input(request, N != 1)
        client.reply_int(client.db.ttl(request[1]))

    @command('keys', True)
    def type(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            result = 'none'
        else:
            result = self._type_name_map[type(value)]
        client.reply_status(result)

    ###########################################################################
    ##    STRING COMMANDS
    @command('strings', True)
    def append(self, client, request, N):
        check_input(request, N != 2,)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            value = bytearray(request[2])
            db._data[key] = value
        elif not isinstance(value, bytearray):
            return client.reply_wrongtype()
        else:
            value.extend(request[2])
        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_int(len(value))

    @command('strings', True)
    def decr(self, client, request, N):
        check_input(request, N != 1)
        r = self._incrby(client, request[0], request[1], b'-1', int)
        client.reply_int(r)

    @command('strings', True)
    def decrby(self, client, request, N):
        check_input(request, N != 2)
        try:
            val = str(-int(request[2])).encode('utf-8')
        except Exception:
            val = request[2]
        r = self._incrby(client, request[0], request[1], val, int)
        client.reply_int(r)

    @command('strings')
    def get(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, bytearray):
            client.reply_bulk(bytes(value))
        else:
            client.reply_wrongtype()

    @command('strings')
    def getbit(self, client, request, N):
        check_input(request, N != 2)
        try:
            offset = int(request[2])
            if offset < 0 or offset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return client.reply_error("Wrong offset in '%s' command" %
                                      request[0])
        string = client.db.get(request[1])
        if string is None:
            client.reply_zero()
        elif not isinstance(string, bytearray):
            client.reply_wrongtype()
        else:
            v = string[offset] if offset < len(string) else 0
            client.reply_int(v)

    @command('strings')
    def getrange(self, client, request, N):
        check_input(request, N != 3)
        try:
            start = int(request[2])
            end = int(request[3])
        except Exception:
            return client.reply_error("Wrong offset in '%s' command" %
                                      request[0])
        string = client.db.get(request[1])
        if string is None:
            client.reply_bulk()
        elif not isinstance(string, bytearray):
            client.reply_wrongtype()
        else:
            if start < 0:
                start = len(string) + start
            if end < 0:
                end = len(string) + end + 1
            else:
                end += 1
            client.reply_bulk(bytes(string[start:end]))

    @command('strings', True)
    def getset(self, client, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            db._data[key] = bytearray(request[2])
            self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
            client.reply_bulk()
        elif isinstance(value, bytearray):
            db.pop(key)
            db._data[key] = bytearray(request[2])
            self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
            client.reply_bulk(bytes(value))
        else:
            client.reply_wrongtype()

    @command('strings', True)
    def incr(self, client, request, N):
        check_input(request, N != 1)
        r = self._incrby(client, request[0], request[1], b'1', int)
        client.reply_int(r)

    @command('strings', True)
    def incrby(self, client, request, N):
        check_input(request, N != 2)
        r = self._incrby(client, request[0], request[1], request[2], int)
        client.reply_int(r)

    @command('strings', True)
    def incrbyfloat(self, client, request, N):
        check_input(request, N != 2)
        r = self._incrby(client, request[0], request[1], request[2], float)
        client.reply_bulk(str(r).encode('utf-8'))

    @command('strings')
    def mget(self, client, request, N):
        check_input(request, not N)
        get = client.db.get
        values = []
        for key in request[1:]:
            value = get(key)
            if value is None:
                values.append(value)
            elif isinstance(value, bytearray):
                values.append(bytes(value))
            else:
                return client.reply_wrongtype()
        client.reply_multibulk(values)

    @command('strings', True)
    def mset(self, client, request, N):
        D = N // 2
        check_input(request, N < 2 or D * 2 != N)
        db = client.db
        for key, value in zip(request[1::2], request[2::2]):
            db.pop(key)
            db._data[key] = bytearray(value)
            self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
        client.reply_ok()

    @command('strings', True)
    def msetnx(self, client, request, N):
        D = N // 2
        check_input(request, N < 2 or D * 2 != N)
        db = client.db
        keys = request[1::2]
        exist = False
        for key in keys:
            exist = db.exists(key)
            if exist:
                break
        if exist:
            client.reply_zero()
        else:
            for key, value in zip(keys, request[2::2]):
                db._data[key] = bytearray(value)
                self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
            client.reply_one()

    @command('strings', True)
    def psetex(self, client, request, N):
        check_input(request, N != 3)
        self._set(client, request[1], request[3], milliseconds=request[2])
        client.reply_ok()

    @command('strings', True)
    def set(self, client, request, N):
        check_input(request, N < 2 or N > 8)
        db = client.db
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
        if self._set(client, request[1], request[2], seconds,
                     milliseconds, nx, xx):
            client.reply_ok()
        else:
            client.reply_bulk()

    @command('strings', True)
    def setbit(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        try:
            offset = int(request[2])
            if offset < 0 or offset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return client.reply_error("Wrong offset in '%s' command" %
                                      request[0])
        try:
            value = int(request[3])
            if value not in (0, 1):
                raise ValueError
        except Exception:
            return client.reply_error("Wrong value in '%s' command" %
                                      request[0])
        db = client.db
        string = db.get(key)
        if string is None:
            string = bytearray(b'\x00')
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return client.reply_wrongtype()
        N = len(string)
        if N < offset:
            string.extend((offset + 1 - N)*b'\x00')
        orig = string[offset]
        string[offset] = value
        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_int(orig)

    @command('strings', True)
    def setex(self, client, request, N):
        check_input(request, N != 3)
        self._set(client, request[1], request[3], seconds=request[2])
        client.reply_ok()

    @command('strings', True)
    def setnx(self, client, request, N):
        check_input(request, N != 2)
        if self._set(client, request[1], request[2], nx=True):
            client.reply_one()
        else:
            client.reply_zero()

    @command('strings', True)
    def setrange(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        value = request[3]
        try:
            offset = int(request[2])
            T = offset + len(value)
            if offset < 0 or T >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return client.reply_error("Wrong offset in '%s' command" %
                                      request[0])
        db = client.db
        string = db.get(key)
        if string is None:
            string = bytearray(b'\x00')
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return client.reply_wrongtype()
        N = len(string)
        if N < T:
            string.extend((T + 1 - N)*b'\x00')
        string[offset:T] = value
        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_int(len(string))

    @command('strings')
    def strlen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, bytearray):
            client.reply_int(len(value))
        else:
            return client.reply_wrongtype()

    ###########################################################################
    ##    HASHES COMMANDS
    @command('hashes', True)
    def hdel(self, client, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.hash_type):
            rem = 0
            for field in request[2:]:
                rem += 0 if value.pop(field, None) is None else 1
            self._signal(self.NOTIFY_HASH, db, request[0], key, rem)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(rem)
        else:
            client.reply_wrongtype()

    @command('hashes')
    def hexists(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.hash_type):
            client.reply_int(int(request[2] in value))
        else:
            client.reply_wrongtype()

    @command('hashes')
    def hget(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, self.hash_type):
            client.reply_bulk(value.get(request[2]))
        else:
            client.reply_wrongtype()

    @command('hashes')
    def hgetall(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multibulk(value.flat())
        else:
            client.reply_wrongtype()

    @command('hashes', True)
    def hincrby(self, client, request, N):
        result = self._hincrby(client, request, N, int)
        client.reply_int(result)

    @command('hashes', True)
    def hincrbyfloat(self, client, request, N):
        result = self._hincrby(client, request, N, float)
        client.reply_bulk(str(result).encode('utf-8'))

    @command('hashes')
    def hkeys(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multibulk(value)
        else:
            client.reply_wrongtype()

    @command('hashes')
    def hlen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.hash_type):
            client.reply_int(len(value))
        else:
            client.reply_wrongtype()

    @command('hashes')
    def hmget(self, client, request, N):
        check_input(request, N < 3)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif isinstance(value, self.hash_type):
            result = value.mget(request[2:])
            client.reply_multibulk(result)
        else:
            client.reply_wrongtype()

    @command('hashes', True)
    def hmset(self, client, request, N):
        D = (N - 1) // 2
        check_input(request, N < 3 or D * 2 != N - 1)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return client.reply_wrongtype()
        it = iter(request[2:])
        value.update(zip(it, it))
        self._signal(self.NOTIFY_HASH, db, request[0], key, D)
        client.reply_ok()

    @command('hashes', True)
    def hset(self, client, request, N):
        check_input(request, N != 3)
        key, field = request[1], request[2]
        db = client.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return client.reply_wrongtype()
        value[field] = request[3]
        self._signal(self.NOTIFY_HASH, db, request[0], key, 1)
        client.reply_zero() if field in value else client.reply_one()

    @command('hashes', True)
    def hsetnx(self, client, request, N):
        check_input(request, N != 3)
        key, field = request[1], request[2]
        db = client.db
        value = db.get(key)
        if value is None:
            value = self.hash_type()
            db._data[key] = value
        elif not isinstance(value, self.hash_type):
            return client.reply_wrongtype()
        if field in value:
            client.reply_zero()
        else:
            value[field] = request[3]
            self._signal(self.NOTIFY_HASH, db, request[0], key, 1)
            client.reply_one()

    @command('hashes')
    def hvals(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multibulk(tuple(value.values()))
        else:
            client.reply_wrongtype()

    ###########################################################################
    ##    LIST COMMANDS
    @command('lists', True, script=0)
    def blpop(self, client, request, N):
        check_input(request, N < 2)
        try:
            timeout = max(0, int(request[-1]))
        except Exception:
            return client.reply_error(self.SYNTAX_ERROR)
        keys = request[1:-1]
        if not self._bpop(client, request[0], keys):
            client.blocked = Blocked(client, request[0], keys, timeout)

    @command('lists', True, script=0)
    def brpop(self, client, request, N):
        return self.blpop(client, request, N)

    @command('lists', True, script=0)
    def brpoplpush(self, client, request, N):
        check_input(request, N != 3)
        try:
            timeout = max(0, int(request[-1]))
        except Exception:
            return client.reply_error(self.SYNTAX_ERROR)
        key, dest = request[1:-1]
        keys = (key,)
        if not self._bpop(client, request[0], keys, dest):
            client.blocked = Blocked(client, request[0], keys, timeout, dest)

    @command('lists')
    def lindex(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, self.list_type):
            index = int(request[2])
            if index >= 0 and index < len(value):
                client.reply_bulk(value[index])
            else:
                client.reply_bulk()
        else:
            client.reply_wrongtype()

    @command('lists', True)
    def linsert(self, client, request, N):
        # This method is INEFFICIENT, but redis supported so we do
        # the same here
        check_input(request, N != 4)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            where = request[2].lower()
            pivot = request[3]
            if where == b'before':
                value.insert_before(request[3], request[4])
            elif where == b'after':
                value.insert_after(request[3], request[4])
            else:
                return client.reply_error('cannot insert to list')
            self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
            client.reply_int(len(value))

    @command('lists')
    def llen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.list_type):
            client.reply_int(len(value))
        else:
            client.reply_wrongtype()

    @command('lists', True)
    def lpop(self, client, request, N):
        check_input(request, N != 1)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_bulk()
        elif isinstance(value, self.list_type):
            if request[1] == 'lpop':
                result = value.popleft()
            else:
                result = value.pop()
            self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_bulk(result)
        else:
            client.reply_wrongtype()

    @command('lists', True)
    def rpop(self, client, request, N):
        return self.lpop(client, request, N)

    @command('lists', True)
    def lpush(self, client, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            value = self.list_type()
            db._data[key] = value
        elif not isinstance(value, self.list_type):
            return client.reply_wrongtype()
        if request[0] == 'lpush':
            value.extendleft(request[2:])
        else:
            value.extend(request[2:])
        client.reply_int(len(value))
        self._signal(self.NOTIFY_LIST, db, request[0], key, N - 1)

    @command('lists', True)
    def rpush(self, client, request, N):
        return self.lpush(client, request, N)

    @command('lists', True)
    def lpushx(self, client, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            if request[0] == 'lpush':
                value.appendleft(request[2])
            else:
                value.append(request[2])
            client.reply_int(len(value))
            self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
    rpushx = lpushx

    @command('lists', True)
    def lrange(self, client, request, N):
        check_input(request, N != 3)
        db = client.db
        key = request[1]
        value = db.get(key)
        try:
            start, end = self._range_values(value, request[2], request[3])
        except Exception:
            return client.reply_error('invalid range')
        if value is None:
            client.reply_multibulk(())
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            client.reply_multibulk(tuple(islice(value, start, end)))

    @command('lists', True)
    def lrem(self, client, request, N):
        # This method is INEFFICIENT, but redis supported so we do
        # the same here
        check_input(request, N != 3)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            try:
                count = int(request[2])
            except Exception:
                return client.reply_error('cannot remove from list')
            removed = value.remove(request[3], count)
            if removed:
                self._signal(self.NOTIFY_LIST, db, request[0], key, removed)
            client.reply_int(removed)

    @command('lists', True)
    def lset(self, client, request, N):
        check_input(request, N != 3)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_error(self.OUT_OF_BOUND)
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            try:
                index = int(request[2])
            except Exception:
                index = -1
            if index >= 0 and index < len(value):
                value[index] = request[3]
                self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
                client.reply_ok()
            else:
                client.reply_error(self.OUT_OF_BOUND)

    @command('lists', True)
    def ltrim(self, client, request, N):
        check_input(request, N != 3)
        db = client.db
        key = request[1]
        value = db.get(key)
        try:
            start, end = self._range_values(value, request[2], request[3])
        except Exception:
            return client.reply_error('invalid range')
        if value is None:
            client.reply_ok()
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            start = len(value)
            value.trim(start, end)
            client.reply_ok()
            self._signal(self.NOTIFY_LIST, db, request[0], key,
                         start-len(value))
            client.reply_ok()

    @command('lists', True)
    def rpoplpush(self, client, request, N):
        check_input(request, N != 2)
        key1, key2 = request[1], request[2]
        db = client.db
        orig = db.get(key1)
        dest = db.get(key2)
        if orig is None:
            client.reply_bulk()
        elif not isinstance(orig, self.list_type):
            client.reply_wrongtype()
        else:
            if dest is None:
                dest = self.list_type()
                db._data[key2] = dest
            elif not isinstance(dest, self.list_type):
                return client.reply_wrongtype()
            value = orig.pop()
            self._signal(self.NOTIFY_LIST, db, 'rpop', key1, 1)
            dest.appendleft(value)
            self._signal(self.NOTIFY_LIST, db, 'lpush', key2, 1)
            if db.pop(key1, orig) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key1)
            client.reply_bulk(value)

    ###########################################################################
    ##    SETS COMMANDS
    @command('sets', True)
    def sadd(self, client, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            value = set()
            db._data[key] = value
        elif not isinstance(value, set):
            return client.reply_wrongtype()
        n = len(value)
        value.update(request[2:])
        n = len(value) - n
        self._signal(self.NOTIFY_SET, db, request[0], key, n)
        client.reply_int(n)

    @command('sets')
    def scard(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_int(len(value))

    @command('sets')
    def sdiff(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'difference', request[1:])

    @command('sets', True)
    def sdiffstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'difference', request[2:], request[1])

    @command('sets')
    def sinter(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'intersection', request[1:])

    @command('sets', True)
    def sinterstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'intersection', request[2:], request[1])

    @command('sets')
    def sismemeber(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_int(int(request[2] in value))

    @command('sets')
    def smembers(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_multibulk(value)

    @command('sets', True)
    def smove(self, client, request, N):
        check_input(request, N != 3)
        db = client.db
        key1 = request[1]
        key2 = request[2]
        orig = db.get(key1)
        dest = db.get(key2)
        if orig is None:
            client.reply_zero()
        elif not isinstance(orig, set):
            client.reply_wrongtype()
        else:
            member = request[3]
            if member in orig:
                # we my be able to move
                if dest is None:
                    dest = set()
                    db._data[request[2]] = dest
                elif not isinstance(dest, set):
                    return client.reply_wrongtype()
                orig.remove(member)
                dest.add(member)
                self._signal(self.NOTIFY_SET, db, 'srem', key1)
                self._signal(self.NOTIFY_SET, db, 'sadd', key2, 1)
                if db.pop(key1, orig) is not None:
                    self._signal(self.NOTIFY_GENERIC, db, 'del', key1)
                client.reply_one()
            else:
                client.reply_zero()

    @command('sets', True)
    def spop(self, client, request, N):
        check_input(request, N != 1)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_bulk()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            result = value.pop()
            self._signal(self.NOTIFY_SET, db, request[0], key, 1)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_bulk(result)

    @command('sets')
    def srandmember(self, client, request, N):
        check_input(request, N < 1 or N > 2)
        value = client.db.get(request[1])
        if value is not None and not isinstance(value, set):
            return client.reply_wrongtype()
        if N == 2:
            try:
                count = int(request[2])
            except Exception:
                return client.reply_error('Invalid count')
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
            client.reply_multibulk(result)
        else:
            if not value:
                result = None
            else:
                result = value.pop()
                value.add(result)
            client.reply_bulk(result)

    @command('sets', True)
    def srem(self, client, request, N):
        check_input(request, N < 2)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            start = len(value)
            value.difference_update(request[2:])
            removed = start - len(value)
            self._signal(self.NOTIFY_SET, db, request[0], key, removed)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(removed)

    @command('sets')
    def sunion(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'union', request[1:])

    @command('sets', True)
    def sunionstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'union', request[2:], request[1])

    ###########################################################################
    ##    SORTED SETS COMMANDS
    @command('Sorted sets', True)
    def zadd(self, client, request, N):
        D = (N - 1) // 2
        check_input(request, N < 3 or D * 2 != N - 1)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            value = self.zset_type()
            db._data[key] = value
        elif not isinstance(value, self.zset_type):
            return client.reply_wrongtype()
        start = len(value)
        value.update(zip(map(float, request[2::2]), request[3::2]))
        result = len(value) - start
        self._signal(self.NOTIFY_ZSET, db, request[0], key, result)
        client.reply_int(result)

    @command('Sorted sets')
    def zcard(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            client.reply_int(len(value))

    @command('Sorted sets')
    def zcount(self, client, request, N):
        check_input(request, N != 3)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                mmin = float(request[2])
                mmax = float(request[3])
            except Exception:
                client.reply_error(self.INVALID_SCORE)
            else:
                client.reply_int(value.count(mmin, mmax))

    @command('Sorted sets', True)
    def zincrby(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            db._data[key] = value = self.zset_type()
        elif not isinstance(value, self.zset_type):
            return client.reply_wrongtype()
        try:
            increment = float(request[2])
        except Exception:
            client.reply_error(self.INVALID_SCORE)
        else:
            member = request[3]
            score = value.score(member, 0) + increment
            value.add(score, member)
            self._signal(self.NOTIFY_ZSET, db, request[0], key, 1)
            client.reply_bulk(str(score).encode('utf-8'))

    @command('Sorted sets')
    def zrange(self, client, request, N):
        check_input(request, N < 3 or N > 4)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multibulk(())
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                start, end = self._range_values(value, request[2], request[3])
            except exception:
                return client.reply_error(self.SYNTAX_ERROR)
            if N == 4:
                if request[4].lower() == b'withscores':
                    result = []
                    [result.extend(vs) for vs in
                     value.range(start, end, scores=True)]
                else:
                    return client.reply_error(self.SYNTAX_ERROR)
            else:
                result = list(value.range(start, end))
            client.reply_multibulk(result)

    @command('Sorted sets')
    def zrank(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            rank = value.rank(request[2])
            if rank is not None:
                client.reply_int(rank)
            else:
                client.reply_bulk()

    @command('Sorted sets', True)
    def zrem(self, client, request, N):
        check_input(request, N < 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            removed = value.remove_items(request[2:])
            if removed:
                self._signal(self.NOTIFY_ZSET, db, request[0], key, removed)
            if not value:
                db.pop()
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(removed)

    ###########################################################################
    ##    PUBSUB COMMANDS
    @command('Pub/Sub', script=0)
    def psubscribe(self, client, request, N):
        check_input(request, not N)
        for pattern in request[1:]:
            p = self._patterns.get(pattern)
            if not p:
                p = pubsub_patterns(re.compile(pattern.decode('utf-8')), set())
                self._patterns[pattern] = p
            p.clients.add(client)
            client.patterns.add(pattern)
            count = reduce(lambda x, y: x + int(client in y.clients),
                           self._patterns.values())
            client.reply_multibulk((b'psubscribe', pattern, count))

    @command('Pub/Sub')
    def pubsub(self, client, request, N):
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
            client.reply_multibulk(channels)
        elif subcommand == 'numsub':
            count = []
            for channel in request[2:]:
                clients = self._channels.get(channel, ())
                count.append(len(clients))
            client.reply_multibulk(count)
        elif subcommand == 'numpat':
            check_input(request, N > 1)
            count = reduce(lambda x, y: x + len(y.clients),
                           self._patterns.values())
            client.reply_int(count)
        else:
            client.reply_error("Unknown command 'pubsub %s'" % subcommand)

    @command('Pub/Sub')
    def publish(self, client, request, N):
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
        client.reply_int(count)

    @command('Pub/Sub', script=0)
    def punsubscribe(self, client, request, N):
        check_input(request, not N)
        patterns = list(self._patterns) if N == 1 else request[1:]
        for pattern in patterns:
            if pattern in self._patterns:
                p = self._patterns[pattern]
                if client in p.clients:
                    client.patterns.discard(pattern)
                    p.clients.remove(client)
                    if not p.clients:
                        self._patterns.pop(pattern)
                    client.reply_multibulk((b'punsubscribe', pattern))

    @command('Pub/Sub', script=0)
    def subscribe(self, client, request, N):
        check_input(request, not N)
        for channel in request[1:]:
            clients = self._channels.get(channel)
            if not clients:
                self._channels[channel] = clients = set()
            clients.add(client)
            client.channels.add(channel)
            client.reply_multibulk((b'subscribe', channel, len(clients)))

    @command('Pub/Sub', script=0)
    def unsubscribe(self, client, request, N):
        check_input(request, not N)
        channels = list(self._channels) if N == 1 else request[1:]
        for channel in channels:
            if channel in self._channels:
                clients = self._channels[channel]
                if client in clients:
                    client.channels.discard(channel)
                    clients.remove(client)
                    if not clients:
                        self._channels.pop(channel)
                    client.reply_multibulk((b'unsubscribe', channel))

    ###########################################################################
    ##    TRANSACTION COMMANDS
    @command('transactions', script=0)
    def discard(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_error("DISCARD without MULTI")
        else:
            self._close_transaction(client)
            client.reply_ok()

    @command('transactions', name='exec', script=0)
    def execute(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_error("EXEC without MULTI")
        else:
            requests = client.transaction
            if client.flag & self.DIRTY_CAS:
                self._close_transaction(client)
                client.reply_multibulk(())
            else:
                self._close_transaction(client)
                client.reply_multibulk_len(len(requests))
                for handle, request in requests:
                    client._execute_command(handle, request)

    @command('transactions', script=0)
    def multi(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_ok()
            client.transaction = []
        else:
            self.error_replay("MULTI calls can not be nested")

    @command('transactions', script=0)
    def watch(self, client, request, N):
        check_input(request, not N)
        if client.transaction is not None:
            client.reply_error("WATCH inside MULTI is not allowed")
        else:
            wkeys = client.watched_keys
            if not wkeys:
                client.watched_keys = wkeys = set()
                self._watching.add(client)
            wkeys.update(request[1:])
            client.reply_ok()

    @command('transactions', script=0)
    def unwatch(self, client, request, N):
        check_input(request, N)
        transaction = client.transaction
        self._close_transaction(client)
        client.transaction = transaction
        client.reply_ok()

    ###########################################################################
    ##    SCRIPTING
    @command('scripting', script=0)
    def eval(self, client, request, N):
        check_input(request, N < 2)
        if not self.lua:
            return client.reply_error(self.SYNTAX_ERROR)
        script = request[1]
        self._eval_script(client, script, request)

    @command('scripting', script=0)
    def evalsha(self, client, request, N):
        check_input(request, N < 2)
        if not self.lua:
            return client.reply_error(self.SYNTAX_ERROR)
        script = self.scripts.get(request[2])
        if script is None:
            client.reply_error('the script is not available', 'NOSCRIPT')
        else:
            self._eval_script(client, script, request)

    @command('scripting', script=0)
    def script(self, client, request, N):
        check_input(request, not N)
        if not self.lua:
            return client.reply_error(self.SYNTAX_ERROR)
        scripts = self.scripts
        subcommand = request[1].upper()
        if subcommand == b'EXISTS':
            check_input(request, N < 2)
            result = [int(sha in scripts) for sha in requests[2:]]
            client.reply_multibulk(result)
        elif subcommand == b'FLUSH':
            check_input(request, N != 1)
            scripts.clear()
            client.reply_ok()
        elif subcommand == b'KILL':
            check_input(request, N != 1)
            client.reply_ok()
        elif subcommand == b'LOAD':
            check_input(request, N != 2)
            script = request[2]
            sha = sha1(script).hexdigest().encode('utf-8')
            scripts[sha] = script
            client.reply_bulk(sha)

    ###########################################################################
    ##    CONNECTION COMMANDS
    @command('connection', script=0)
    def auth(self, client, request, N):
        check_input(request, N != 1)
        conn.password = request[1]
        if conn.password != conn._producer.password:
            client.reply_error("wrong password")
        else:
            client.reply_ok()

    @command('connection')
    def echo(self, client, request, N):
        check_input(request, N != 1)
        client.reply_bulk(request[1])

    @command('connection')
    def ping(self, client, request, N):
        check_input(request, N)
        client.reply_status('PONG')

    @command('connection', script=0)
    def quit(self, client, request, N):
        check_input(request, N)
        client.reply_ok()
        client.close()

    @command('connection')
    def select(self, client, request, N):
        check_input(request, N != 1)
        D = len(self.databases) - 1
        try:
            num = int(request[1])
            if num < 0 or num > D:
                raise ValueError
        except ValueError:
            client.reply_error(('select requires a database number between '
                                '%s and %s' % (0, D)))
        else:
            client.database = num
            client.reply_ok()

    ###########################################################################
    ##    SERVER COMMANDS
    @command('server')
    def bgsave(self, client, request, N):
        check_input(request, N)
        self._save()
        client.reply_ok()

    @command('server')
    def client(self, client, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'list':
            check_input(request, N != 1)
            value = '\n'.join(self._client_list(client))
            client.reply_bulk(value.encode('utf-8'))
        else:
            client.reply_error("unknown command 'client %s'" % subcommand)

    @command('server')
    def config(self, client, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'get':
            if N != 2:
                client.reply_error("'config get' no argument")
            else:
                value = self._get_config(request[2].decode('utf-8'))
                client.reply_bulk(value)
        elif subcommand == 'rewrite':
            client.reply_ok()
        elif subcommand == 'set':
            try:
                if N != 3:
                    raise ValueError("'config set' no argument")
                self._set_config(request[2].decode('utf-8'))
            except Exception as e:
                client.reply_error(str(e))
            else:
                client.reply_ok()
        elif subcommand == 'resetstat':
            self._hit_keys = 0
            self._missed_keys = 0
            self._expired_keys = 0
            server = client._producer
            server._received = 0
            server._requests_processed = 0
            client.reply_ok()
        else:
            client.reply_error("'config %s' not valid" % subcommand)

    @command('server')
    def dbsize(self, client, request, N):
        check_input(request, N != 0)
        client.reply_int(len(client.db))

    @command('server', True)
    def flushdb(self, client, request, N):
        check_input(request, N)
        client.db.flush()
        client.reply_ok()

    @command('server', True)
    def flushall(self, client, request, N):
        check_input(request, N)
        for db in self.databases.values():
            db.flush()
        client.reply_ok()

    @command('server')
    def info(self, client, request, N):
        check_input(request, N)
        info = '\n'.join(self._flat_info())
        client.reply_bulk(info.encode('utf-8'))

    @command('server', script=0)
    def monitor(self, client, request, N):
        check_input(request, N)
        client.flag |= MONITOR
        self._monitors.add(client)
        client.reply_ok()

    @command('server', script=0)
    def save(self, client, request, N):
        check_input(request, N)
        self._save(False)
        client.reply_ok()

    @command('server')
    def time(self, client, request, N):
        check_input(request, N != 0)
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        client.reply_multibulk((seconds, microseconds))

    ###########################################################################
    ##    INTERNALS
    def _cron(self):
        dirty = self._dirty
        if dirty:
            now = time.time()
            gap = now - self._last_save
            for interval, changes in self.cfg.key_value_save:
                if gap >= interval and dirty >= changes:
                    self._save()
                    break

    def _set(self, client, key, value, seconds=0, milliseconds=0,
             nx=False, xx=False):
        try:
            seconds = int(seconds)
            milliseconds = 0.000001*int(milliseconds)
            if seconds < 0 or milliseconds < 0:
                raise ValueError
        except Exception:
            return client.reply_error('invalid expire time')
        else:
            timeout = seconds + milliseconds
        db = client.db
        exists = db.exists(key)
        skip = (exists and nx) or (not exists and xx)
        if not skip:
            if exists:
                db.pop(key)
            if timeout > 0:
                db._expires[key] = (self._loop.call_later(
                    timeout, db._do_expire, key), bytearray(value))
                self._signal(self.NOTIFY_STRING, db, 'expire', key)
            else:
                db._data[key] = bytearray(value)
            self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
            return True

    def _incrby(self, client, name, key, value, type):
        try:
            tv = type(value)
        except Exception:
            return client.reply_error('invalid increment')
        db = client.db
        cur = db.get(key)
        if cur is None:
            db._data[key] = bytearray(value)
        elif isinstance(cur, bytearray):
            try:
                tv += type(cur)
            except Exception:
                return client.reply_error('invalid increment')
            db._data[key] = bytearray(str(tv).encode('utf-8'))
        else:
            return client.reply_wrongtype()
        self._signal(self.NOTIFY_STRING, db, name, key, 1)
        return tv

    def _bpop(self, client, request, keys, dest=None):
        list_type = self.list_type
        db = client.db
        for key in keys:
            value = db.get(key)
            if isinstance(value, list_type):
                self._block_callback(client, request[0], key, value, dest)
                return True
            elif value is not None:
                client.reply_wrongtype()
                return True
        return False

    def _block_callback(self, client, command, key, value, dest):
        db = client.db
        if command[:2] == 'br':
            if dest is not None:
                dval = db.get(dest)
                if dval is None:
                    dval = self.list_type()
                    db._data[dest] = dval
                elif not isinstance(dval, self.list_type):
                    return client.reply_wrongtype()
            elem = value.pop()
            self._signal(self.NOTIFY_LIST, db, 'rpop', key, 1)
            if dest is not None:
                dval.appendleft(elem)
                self._signal(self.NOTIFY_LIST, db, 'lpush', dest, 1)
        else:
            elem = value.popleft()
            self._signal(self.NOTIFY_LIST, db, 'lpop', key, 1)
        if not value:
            db.pop(key)
            self._signal(self.NOTIFY_GENERIC, db, 'del', key, 1)
        if dest is None:
            client.reply_multibulk((key, elem))
        else:
            client.reply_bulk(elem)

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

    def _close_transaction(self, client):
        client.transaction = None
        client.watched_keys = None
        client.flag &= ~self.DIRTY_CAS
        self._watching.discard(client)

    def _flat_info(self):
        info = self._server.info()
        info['server']['redis_version'] = self.version
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

    def _hincrby(self, client, request, N, type):
        check_input(request, N != 3)
        key, field, increment = request[1], request[2], type(request[3])
        db = client.db
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
            client.reply_wrongtype()
        return result

    def _setoper(self, client, oper, keys, dest=None):
        db = client.db
        result = None
        for key in keys:
            value = db.get(key)
            if value is None:
                value = set()
            elif not isinstance(value, set):
                return client.reply_wrongtype()
            if result is None:
                result = value
            else:
                result = getattr(result, oper)(value)
        if dest is not None:
            db.pop(dest)
            if result:
                db._data[dest] = result
                client.reply_int(len(result))
            else:
                client.reply_zero()
        else:
            client.reply_multi_bulk(result)

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

    def _client_list(self, client):
        for client in client._producer._concurrent_connections:
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
                client._transport.write(msg)
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
                client.flag |= self.DIRTY_CAS

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
        # the key is blocking clients
        if key in db._blocking_keys:
            if key in db._data:
                value = db._data[key]
            elif key in self._expires:
                value = db._expires[key]
            else:
                value = None
            for client in db._blocking_keys.pop(key):
                client.blocked.unblock(client, key, value)

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

    def _write_to_monitors(self, client, request):
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

    def _eval_script(self, client, script, request):
        try:
            numkeys = int(request[2])
            if numkeys > 0:
                keys = request[3:3+numkeys]
                if len(keys) < numkeys:
                    raise ValueError
            elif numkeys < 0:
                raise ValueError
            else:
                keys = []
        except Exception:
            return client.reply_error(self.SYNTAX_ERROR)
        args = request[3+numkeys:]
        self.lua.set_global('KEYS', keys)
        self.lua.set_global('ARGV', args)
        result = self.lua.execute(script)
        if isinstance(result, dict):
            if len(result) == 1:
                key = tuple(result)[0]
                keyu = key.upper()
                if keyu == b'OK':
                    return client.reply_ok()
                elif keyu == b'ERR':
                    return client.reply_error(result[key])
                else:
                    return client.reply_multibulk(())
            else:
                array = []
                index = 0
                while result:
                    index += 1
                    value = result.pop(index, None)
                    if value is None:
                        break
                    array.append(value)
                result = array
        if isinstance(result, list):
            client.reply_multibulk(result)
        elif result == True:
            client.reply_one()
        elif result == False:
            client.reply_bulk()
        elif isinstance(result, (int, float)):
            client.reply_int(int(result))
        else:
            client.reply_bulk(result)


class Db(object):
    '''The database.
    '''
    def __init__(self, num, store):
        self.store = store
        self._num = num
        self._loop = store._loop
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
        self.store._signal(self.store.NOTIFY_GENERIC, self, 'flushdb',
                           dirty=removed)

    def get(self, key, default=None):
        if key in self._data:
            self.store._hit_keys += 1
            return self._data[key]
        elif key in self._expires:
            self.store._hit_keys += 1
            return self._expires[key]
        else:
            self.store._missed_keys += 1
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
        self._expires[key] = (self._loop.call_later(
            timeout - time.time(), self._do_expire, key), value)
        return True

    def persist(self, key):
        if key in self._expires:
            self.store._hit_keys += 1
            handle, value = self._expires.pop(key)
            handle.cancel()
            self._data[key] = value
            return True
        elif key in self._data:
            self.store._hit_keys += 1
        else:
            self.store._missed_keys += 1
        return False

    def ttl(self, key):
        if key in self._expires:
            self.store._hit_keys += 1
            handle, value = self._expires[key]
            return max(0, int(handle._when - self._loop.time()))
        elif key in self._data:
            self.store._hit_keys += 1
            return -1
        else:
            self.store._missed_keys += 1
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
            self.store._hit_keys += 1
            self._data.pop(key)
            self.store._signal(self.store.NOTIFY_GENERIC, self, 'del', key, 1)
            return 1
        elif key in self._expires:
            self.store._hit_keys += 1
            handle, _, self._expires.pop(key)
            handle.cancel()
            self.store._signal(self.store.NOTIFY_GENERIC, self, 'del', key, 1)
            return 1
        else:
            self.store._missed_keys += 1
            return 0

    def _do_expire(self, key):
        if key in self._expires:
            handle, value, = self._expires.pop(key)
            handle.cancel()
            self.store._expired_keys += 1


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
