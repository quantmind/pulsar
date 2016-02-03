'''
Pulsar-ds is a python implementation of the popular redis_
data store. It uses pulsar asynchronous framework to create a
single-threaded worker responding to TCP-requests in the same way
as redis does.

To run a stand alone server create a script with the following code::


    from pulsar.apps.data import PulsarDS

    if __name__ == '__main__':
        PulsarDS().start()


More information on the :ref:`pulsar data store example <tutorials-pulsards>`.

Check out these benchmarks_

.. _benchmarks: https://gist.github.com/lsbardel/8068579

Implementation
===========================

Pulsar Data Store Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PulsarDS
   :members:
   :member-order: bysource


Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Storage
   :members:
   :member-order: bysource


.. _redis: http://redis.io/
'''
import os
import re
import time
import math
import pickle
from random import choice
from itertools import islice, chain
from functools import partial, reduce
from collections import namedtuple
from itertools import zip_longest

import pulsar
from pulsar.apps.socket import SocketServer
from pulsar.utils.config import Global
from pulsar.utils.structures import Dict, Zset, Deque

from .parser import redis_parser
from .utils import sort_command, count_bytes, and_op, or_op, xor_op, save_data
from .client import (command, PulsarStoreClient, Blocked,
                     COMMANDS_INFO, check_input, redis_to_py_pattern)


DEFAULT_PULSAR_STORE_ADDRESS = '127.0.0.1:6410'


def pulsards_url(address=None):
    if not address:
        actor = pulsar.get_actor()
        if actor:
            address = actor.cfg.data_store
    address = address or DEFAULT_PULSAR_STORE_ADDRESS
    if not address.startswith('pulsar://'):
        address = 'pulsar://%s' % address
    return address


# Keyspace changes notification classes
STRING_LIMIT = 2**32

nan = float('nan')


class RedisParserSetting(Global):
    name = "redis_py_parser"
    flags = ["--redis-py-parser"]
    action = "store_true"
    default = False
    desc = '''\
    Use the python redis parser rather the C implementation.

    Mainly used for benchmarking purposes.
    '''


class PulsarDsSetting(pulsar.Setting):
    virtual = True
    app = 'pulsards'
    section = "Pulsar data store server"


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


# #############################################################################
# #    CONFIGURATION PARAMETERS
class KeyValueDatabases(PulsarDsSetting):
    name = "key_value_databases"
    flags = ["--key-value-databases"]
    type = int
    default = 16
    desc = 'Number of databases for the key value store.'


class KeyValuePassword(PulsarDsSetting):
    name = "key_value_password"
    flags = ["--key-value-password"]
    default = ''
    desc = 'Optional password for the database.'


class KeyValueSave(PulsarDsSetting):
    name = "key_value_save"
    # default = [(900, 1), (300, 10), (60, 10000)]
    default = []
    validator = validate_list_of_pairs
    desc = '''\
        List of pairs controlling data store persistence.

        Will save the DB if both the given number of seconds and the given
        number of write operations against the DB occurred.

        The default behaviour will be to save:
        after 900 sec (15 min) if at least 1 key changed
        after 300 sec (5 min) if at least 10 keys changed
        after 60 sec if at least 10000 keys changed

        You can disable saving at all by setting an empty list
    '''


class KeyValueFileName(PulsarDsSetting):
    name = "key_value_filename"
    flags = ["--key-value-filename"]
    default = 'pulsards.rdb'
    desc = '''The filename where to dump the DB.'''


class TcpServer(pulsar.TcpServer):

    def __init__(self, cfg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cfg = cfg
        self._parser_class = redis_parser(cfg.redis_py_parser)
        self._key_value_store = Storage(self, cfg)

    def info(self):
        info = super().info()
        info.update(self._key_value_store._info())
        return info


class PulsarDS(SocketServer):
    '''A :class:`.SocketServer` serving a pulsar datastore.
    '''
    name = 'pulsards'
    cfg = pulsar.Config(bind=DEFAULT_PULSAR_STORE_ADDRESS,
                        keep_alive=0,
                        apps=['socket', 'pulsards'])

    def server_factory(self, *args, **kw):
        return TcpServer(self.cfg, *args, **kw)

    def protocol_factory(self):
        return partial(PulsarStoreClient, self.cfg)

    def monitor_start(self, monitor):
        cfg = self.cfg
        workers = min(1, cfg.workers)
        cfg.set('workers', workers)
        return super().monitor_start(monitor)


# #############################################################################
# #    DATA STORE
pubsub_patterns = namedtuple('pubsub_patterns', 're clients')


class Storage:
    '''Implement redis commands.
    '''
    def __init__(self, server, cfg):
        self.cfg = cfg
        self._password = cfg.key_value_password.encode('utf-8')
        self._filename = cfg.key_value_filename
        self._writer = None
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
        self.logger = server.logger
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
        self._set_options = (b'ex', b'px', b'nx', b'xx')
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
        self.NOT_SUPPORTED = 'Command not yet supported'
        self.OUT_OF_BOUND = 'Out of bound'
        self.SYNTAX_ERROR = 'Syntax error'
        self.SUBSCRIBE_COMMANDS = ('psubscribe', 'punsubscribe', 'subscribe',
                                   'unsubscribe', 'quit')
        self.encoder = pickle
        self.hash_type = Dict
        self.list_type = Deque
        self.zset_type = Zset
        self.data_types = (bytearray, set, self.hash_type,
                           self.list_type, self.zset_type)
        self.zset_aggregate = {b'min': min,
                               b'max': max,
                               b'sum': sum}
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
        self.lua = None
        self.version = '2.4.10'
        self._loaddb()
        self._cron()

    # #########################################################################
    # #    KEYS COMMANDS
    @command('Keys', True, name='del')
    def delete(self, client, request, N):
        check_input(request, not N)
        rem = client.db.rem
        result = reduce(lambda x, y: x + rem(y), request[1:], 0)
        client.reply_int(result)

    @command('Keys')
    def dump(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        else:
            client.reply_bulk(self.encoder.dumps(value))

    @command('Keys')
    def exists(self, client, request, N):
        check_input(request, N != 1)
        if client.db.exists(request[1]):
            client.reply_one()
        else:
            client.reply_zero()

    @command('Keys', True)
    def expire(self, client, request, N, m=1):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            client.reply_error(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return client.reply_error(self.INVALID_TIMEOUT)
                if client.db.expire(request[1], m*timeout):
                    return client.reply_one()
            client.reply_zero()

    @command('Keys', True)
    def expireat(self, client, request, N, M=1):
        check_input(request, N != 2)
        try:
            timeout = int(request[2])
        except ValueError:
            client.reply_error(self.INVALID_TIMEOUT)
        else:
            if timeout:
                if timeout < 0:
                    return client.reply_error(self.INVALID_TIMEOUT)
                timeout = M*timeout - time.time()
                if client.db.expire(request[1], timeout):
                    return client.reply_one()
            client.reply_zero()

    @command('Keys')
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
        client.reply_multi_bulk(result)

    @command('Keys', supported=False)
    def migrate(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Keys', True)
    def move(self, client, request, N):
        check_input(request, N != 2)
        key = request[1]
        try:
            db2 = self.databases.get(int(request[2]))
            if db2 is None:
                raise ValueError
        except Exception:
            return client.reply_error('index out of range')
        db = client.db
        value = db.get(key)
        if db2.exists(key) or value is None:
            return client.reply_zero()
        assert value
        db.pop(key)
        self._signal(self.NOTIFY_GENERIC, db, 'del', key, 1)
        db2._data[key] = value
        self._signal(self._type_event_map[type(value)], db2, 'set', key, 1)
        client.reply_one()

    @command('Keys', supported=False)
    def object(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Keys', True)
    def persist(self, client, request, N):
        check_input(request, N != 1)
        if client.db.persist(request[1]):
            client.reply_one()
        else:
            client.reply_zero()

    @command('Keys', True)
    def pexpire(self, client, request, N):
        self.expire(client, request, N, 0.001)

    @command('Keys', True)
    def pexpireat(self, client, request, N, M=1):
        self.expireat(client, request, N, 0.001)

    @command('Keys')
    def pttl(self, client, request, N):
        check_input(request, N != 1)
        client.reply_int(client.db.ttl(request[1], 1000))

    @command('Keys')
    def randomkey(self, client, request, N):
        check_input(request, N)
        keys = list(client.db)
        if keys:
            client.reply_bulk(choice(keys))
        else:
            client.reply_bulk()

    @command('Keys', True)
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
            assert value
            if ex:
                if db.exists(key2):
                    return client.reply_zero()
                result = 1
            else:
                result = 0
                if db.pop(key2) is not None:
                    self._signal(self.NOTIFY_GENERIC, db, 'del', key2)
            db.pop(key1)
            event = self._type_event_map[type(value)]
            dirty = 1 if event == self.NOTIFY_STRING else len(value)
            db._data[key2] = value
            self._signal(event, db, request[0], key2, dirty)
            client.reply_one() if result else client.reply_ok()

    @command('Keys', True)
    def renamenx(self, client, request, N):
        self.rename(client, request, N, True)

    @command('Keys', True)
    def restore(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        db = client.db
        try:
            value = self.encoder.loads(request[3])
        except Exception:
            value = None
        if not isinstance(value, self.data_types):
            return client.reply_error('Could not decode value')
        try:
            ttl = int(request[2])
        except Exception:
            return client.reply_error(self.INVALID_TIMEOUT)
        if db.pop(key) is not None:
            self._signal(self.NOTIFY_GENERIC, db, 'del', key)
        db._data[key] = value
        if ttl > 0:
            db.expire(key, ttl)
        client.reply_ok()

    @command('Keys', True)
    def sort(self, client, request, N):
        check_input(request, not N)
        value = client.db.get(request[1])
        if value is None:
            value = self.list_type()
        elif not isinstance(value, (set, self.list_type, self.zset_type)):
            return client.reply_wrongtype()
        sort_command(self, client, request, value)

    @command('Keys', True)
    def ttl(self, client, request, N):
        check_input(request, N != 1)
        client.reply_int(client.db.ttl(request[1]))

    @command('Keys', True)
    def type(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            result = 'none'
        else:
            result = self._type_name_map[type(value)]
        client.reply_status(result)

    @command('Keys', supported=False)
    def scan(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    # #########################################################################
    # #    STRING COMMANDS
    @command('Strings', True)
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
            assert value
            value.extend(request[2])
        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_int(len(value))

    @command('Strings')
    def bitcount(self, client, request, N):
        check_input(request, N < 1 or N > 3)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_int(0)
        elif not isinstance(value, bytearray):
            return client.reply_wrongtype()
        else:
            assert value
            if N > 1:
                start = request[2]
                end = request[3] if N == 3 else -1
                start, end = self._range_values(value, start, end)
                value = value[start:end]
            client.reply_int(count_bytes(value))

    @command('Strings', True)
    def bitop(self, client, request, N):
        check_input(request, N < 3)
        db = client.db
        op = request[1].lower()
        if op == b'and':
            reduce_op = and_op
        elif op == b'or':
            reduce_op = or_op
        elif op == b'xor':
            reduce_op = xor_op
        elif op == b'not':
            reduce_op = None
            check_input(request, N != 3)
        else:
            return client.reply_error('bad command')
        empty = bytearray()
        keys = []
        for key in request[3:]:
            value = db.get(key)
            if value is None:
                keys.append(empty)
            elif isinstance(value, bytearray):
                keys.append(value)
            else:
                return client.reply_wrongtype()
        result = bytearray()
        if reduce_op is None:
            for value in keys[0]:
                result.append(~value & 255)
        else:
            for values in zip_longest(*keys, **{'fillvalue': 0}):
                result.append(reduce(reduce_op, values))
        if result:
            dest = request[2]
            if db.pop(dest):
                self._signal(self.NOTIFY_GENERIC, db, 'del', dest)
            db._data[dest] = result
            self._signal(self.NOTIFY_STRING, db, 'set', dest, 1)
            client.reply_int(len(result))
        else:
            client.reply_zero()

    @command('Strings', True)
    def decr(self, client, request, N):
        check_input(request, N != 1)
        r = self._incrby(client, request[0], request[1], b'-1', int)
        client.reply_int(r)

    @command('Strings', True)
    def decrby(self, client, request, N):
        check_input(request, N != 2)
        try:
            val = str(-int(request[2])).encode('utf-8')
        except Exception:
            val = request[2]
        r = self._incrby(client, request[0], request[1], val, int)
        client.reply_int(r)

    @command('Strings')
    def get(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, bytearray):
            assert value
            client.reply_bulk(bytes(value))
        else:
            client.reply_wrongtype()

    @command('Strings')
    def getbit(self, client, request, N):
        check_input(request, N != 2)
        try:
            bitoffset = int(request[2])
            if bitoffset < 0 or bitoffset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return client.reply_error(
                "bit offset is not an integer or out of range")
        string = client.db.get(request[1])
        if string is None:
            client.reply_zero()
        elif not isinstance(string, bytearray):
            client.reply_wrongtype()
        else:
            assert string
            byte = bitoffset >> 3
            if len(string) > byte:
                bit = 7 - (bitoffset & 7)
                v = string[byte] & (1 << bit)
                client.reply_int(1 if v else 0)
            else:
                client.reply_zero()

    @command('Strings')
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
            client.reply_bulk(b'')
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

    @command('Strings', True)
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

    @command('Strings', True)
    def incr(self, client, request, N):
        check_input(request, N != 1)
        r = self._incrby(client, request[0], request[1], b'1', int)
        client.reply_int(r)

    @command('Strings', True)
    def incrby(self, client, request, N):
        check_input(request, N != 2)
        r = self._incrby(client, request[0], request[1], request[2], int)
        client.reply_int(r)

    @command('Strings', True)
    def incrbyfloat(self, client, request, N):
        check_input(request, N != 2)
        r = self._incrby(client, request[0], request[1], request[2], float)
        client.reply_bulk(str(r).encode('utf-8'))

    @command('Strings')
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
        client.reply_multi_bulk(values)

    @command('Strings', True)
    def mset(self, client, request, N):
        D = N // 2
        check_input(request, N < 2 or D * 2 != N)
        db = client.db
        for key, value in zip(request[1::2], request[2::2]):
            db.pop(key)
            db._data[key] = bytearray(value)
            self._signal(self.NOTIFY_STRING, db, 'set', key, 1)
        client.reply_ok()

    @command('Strings', True)
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

    @command('Strings', True)
    def psetex(self, client, request, N):
        check_input(request, N != 3)
        self._set(client, request[1], request[3], milliseconds=request[2])
        client.reply_ok()

    @command('Strings', True)
    def set(self, client, request, N):
        check_input(request, N < 2 or N > 8)
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

    @command('Strings', True)
    def setbit(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        try:
            bitoffset = int(request[2])
            if bitoffset < 0 or bitoffset >= STRING_LIMIT:
                raise ValueError
        except Exception:
            return client.reply_error(
                "bit offset is not an integer or out of range")
        try:
            value = int(request[3])
            if value not in (0, 1):
                raise ValueError
        except Exception:
            return client.reply_error("bit is not an integer or out of range")
        db = client.db
        string = db.get(key)
        if string is None:
            string = bytearray()
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return client.reply_wrongtype()
        else:
            assert string

        # grow value to the right if necessary
        byte = bitoffset >> 3
        num_bytes = len(string)
        if byte >= num_bytes:
            string.extend((byte + 1 - num_bytes)*b'\x00')

        # get current value
        byteval = string[byte]
        bit = 7 - (bitoffset & 7)
        bitval = byteval & (1 << bit)

        # update with new value
        byteval &= ~(1 << bit)
        byteval |= ((value & 1) << bit)
        string[byte] = byteval

        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_one() if bitval else client.reply_zero()

    @command('Strings', True)
    def setex(self, client, request, N):
        check_input(request, N != 3)
        self._set(client, request[1], request[3], seconds=request[2])
        client.reply_ok()

    @command('Strings', True)
    def setnx(self, client, request, N):
        check_input(request, N != 2)
        if self._set(client, request[1], request[2], nx=True):
            client.reply_one()
        else:
            client.reply_zero()

    @command('Strings', True)
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
            string = bytearray(b'')
            db._data[key] = string
        elif not isinstance(string, bytearray):
            return client.reply_wrongtype()
        N = len(string)
        if N < T:
            string.extend((T - N)*b'\x00')
        string[offset:T] = value
        self._signal(self.NOTIFY_STRING, db, request[0], key, 1)
        client.reply_int(len(string))

    @command('Strings')
    def strlen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, bytearray):
            client.reply_int(len(value))
        else:
            return client.reply_wrongtype()

    # #########################################################################
    # #    HASHES COMMANDS
    @command('Hashes', True)
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

    @command('Hashes')
    def hexists(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.hash_type):
            client.reply_int(int(request[2] in value))
        else:
            client.reply_wrongtype()

    @command('Hashes')
    def hget(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, self.hash_type):
            client.reply_bulk(value.get(request[2]))
        else:
            client.reply_wrongtype()

    @command('Hashes')
    def hgetall(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multi_bulk(value.flat())
        else:
            client.reply_wrongtype()

    @command('Hashes', True)
    def hincrby(self, client, request, N):
        result = self._hincrby(client, request, N, int)
        if result is not None:
            client.reply_int(result)

    @command('Hashes', True)
    def hincrbyfloat(self, client, request, N):
        result = self._hincrby(client, request, N, float)
        if result is not None:
            client.reply_bulk(str(result).encode('utf-8'))

    @command('Hashes')
    def hkeys(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multi_bulk(value)
        else:
            client.reply_wrongtype()

    @command('Hashes')
    def hlen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.hash_type):
            client.reply_int(len(value))
        else:
            client.reply_wrongtype()

    @command('Hashes')
    def hmget(self, client, request, N):
        check_input(request, N < 3)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif isinstance(value, self.hash_type):
            result = value.mget(request[2:])
            client.reply_multi_bulk(result)
        else:
            client.reply_wrongtype()

    @command('Hashes', True)
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

    @command('Hashes', True)
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
        avail = (field in value)
        value[field] = request[3]
        self._signal(self.NOTIFY_HASH, db, request[0], key, 1)
        client.reply_zero() if avail else client.reply_one()

    @command('Hashes', True)
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

    @command('Hashes')
    def hvals(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif isinstance(value, self.hash_type):
            client.reply_multi_bulk(tuple(value.values()))
        else:
            client.reply_wrongtype()

    @command('Hashes', supported=False)
    def hscan(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    # #########################################################################
    # #    LIST COMMANDS
    @command('Lists', True, script=0)
    def blpop(self, client, request, N):
        check_input(request, N < 2)
        try:
            timeout = max(0, int(request[-1]))
        except Exception:
            return client.reply_error(self.SYNTAX_ERROR)
        keys = request[1:-1]
        if not self._bpop(client, request, keys):
            client.blocked = Blocked(client, request[0], keys, timeout)

    @command('Lists', True, script=0)
    def brpop(self, client, request, N):
        return self.blpop(client, request, N)

    @command('Lists', True, script=0)
    def brpoplpush(self, client, request, N):
        check_input(request, N != 3)
        try:
            timeout = max(0, int(request[-1]))
        except Exception:
            return client.reply_error(self.SYNTAX_ERROR)
        key, dest = request[1:-1]
        keys = (key,)
        if not self._bpop(client, request, keys, dest):
            client.blocked = Blocked(client, request[0], keys, timeout, dest)

    @command('Lists')
    def lindex(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_bulk()
        elif isinstance(value, self.list_type):
            assert value
            index = int(request[2])
            if index >= 0 and index < len(value):
                client.reply_bulk(value[index])
            else:
                client.reply_bulk()
        else:
            client.reply_wrongtype()

    @command('Lists', True)
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
            assert value
            where = request[2].lower()
            l1 = len(value)
            if where == b'before':
                value.insert_before(request[3], request[4])
            elif where == b'after':
                value.insert_after(request[3], request[4])
            else:
                return client.reply_error('cannot insert to list')
            l2 = len(value)
            if l2 - l1:
                self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
                client.reply_int(l2)
            else:
                client.reply_int(-1)

    @command('Lists')
    def llen(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif isinstance(value, self.list_type):
            assert value
            client.reply_int(len(value))
        else:
            client.reply_wrongtype()

    @command('Lists', True)
    def lpop(self, client, request, N):
        check_input(request, N != 1)
        db = client.db
        key = request[1]
        value = db.get(key)
        if value is None:
            client.reply_bulk()
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            assert value
            if request[0] == 'lpop':
                result = value.popleft()
            else:
                result = value.pop()
            self._signal(self.NOTIFY_LIST, db, request[0], key, 1)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_bulk(result)

    @command('Lists', True)
    def rpop(self, client, request, N):
        return self.lpop(client, request, N)

    @command('Lists', True)
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
        else:
            assert value
        if request[0] == 'lpush':
            value.extendleft(request[2:])
        else:
            value.extend(request[2:])
        client.reply_int(len(value))
        self._signal(self.NOTIFY_LIST, db, request[0], key, N - 1)

    @command('Lists', True)
    def rpush(self, client, request, N):
        return self.lpush(client, request, N)

    @command('Lists', True)
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
            assert value
            if request[0] == 'lpushx':
                value.appendleft(request[2])
            else:
                value.append(request[2])
            client.reply_int(len(value))
            self._signal(self.NOTIFY_LIST, db, request[0], key, 1)

    @command('Lists', True)
    def rpushx(self, client, request, N):
        return self.lpushx(client, request, N)

    @command('Lists', True)
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
            client.reply_multi_bulk(())
        elif not isinstance(value, self.list_type):
            client.reply_wrongtype()
        else:
            assert value
            client.reply_multi_bulk(tuple(islice(value, start, end)))

    @command('Lists', True)
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
            assert value
            try:
                count = int(request[2])
            except Exception:
                return client.reply_error('cannot remove from list')
            removed = value.remove(request[3], count)
            if removed:
                self._signal(self.NOTIFY_LIST, db, request[0], key, removed)
            client.reply_int(removed)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)

    @command('Lists', True)
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
            assert value
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

    @command('Lists', True)
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
            assert value
            value.trim(start, end)
            self._signal(self.NOTIFY_LIST, db, request[0], key,
                         start-len(value))
            client.reply_ok()
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)

    @command('Lists', True)
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
            assert orig
            if dest is None:
                dest = self.list_type()
                db._data[key2] = dest
            elif not isinstance(dest, self.list_type):
                return client.reply_wrongtype()
            else:
                assert dest
            value = orig.pop()
            self._signal(self.NOTIFY_LIST, db, 'rpop', key1, 1)
            dest.appendleft(value)
            self._signal(self.NOTIFY_LIST, db, 'lpush', key2, 1)
            if db.pop(key1, orig) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key1)
            client.reply_bulk(value)

    # #########################################################################
    # #    SETS COMMANDS
    @command('Sets', True)
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

    @command('Sets')
    def scard(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_int(len(value))

    @command('Sets')
    def sdiff(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'difference', request[1:])

    @command('Sets', True)
    def sdiffstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'difference', request[2:], request[1])

    @command('Sets')
    def sinter(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'intersection', request[1:])

    @command('Sets', True)
    def sinterstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'intersection', request[2:], request[1])

    @command('Sets')
    def sismember(self, client, request, N):
        check_input(request, N != 2)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_int(int(request[2] in value))

    @command('Sets')
    def smembers(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif not isinstance(value, set):
            client.reply_wrongtype()
        else:
            client.reply_multi_bulk(value)

    @command('Sets', True)
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

    @command('Sets', True)
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

    @command('Sets')
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
            client.reply_multi_bulk(result)
        else:
            if not value:
                result = None
            else:
                result = value.pop()
                value.add(result)
            client.reply_bulk(result)

    @command('Sets', True)
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

    @command('Sets')
    def sunion(self, client, request, N):
        check_input(request, N < 1)
        self._setoper(client, 'union', request[1:])

    @command('Sets', True)
    def sunionstore(self, client, request, N):
        check_input(request, N < 2)
        self._setoper(client, 'union', request[2:], request[1])

    @command('Sets', supported=False)
    def sscan(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    # #########################################################################
    # #    SORTED SETS COMMANDS
    @command('Sorted Sets', True)
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

    @command('Sorted Sets')
    def zcard(self, client, request, N):
        check_input(request, N != 1)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            client.reply_int(len(value))

    @command('Sorted Sets')
    def zcount(self, client, request, N):
        check_input(request, N != 3)
        value = client.db.get(request[1])
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            min_value, max_value = request[2], request[3]
            include_min = include_max = True
            if min_value and min_value[0] == 40:
                include_min = False
                min_value = min_value[1:]
            if max_value and max_value[0] == 40:
                include_max = False
                max_value = max_value[1:]
            try:
                mmin = float(min_value)
                mmax = float(max_value)
            except Exception:
                client.reply_error(self.INVALID_SCORE)
            else:
                client.reply_int(value.count(mmin, mmax,
                                             include_min, include_max))

    @command('Sorted Sets', True)
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

    @command('Sorted Sets', True)
    def zinterstore(self, client, request, N):
        self._zsetoper(client, request, N)

    @command('Sorted Sets')
    def zrange(self, client, request, N):
        check_input(request, N < 3 or N > 4)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                start, end = self._range_values(value, request[2], request[3])
            except Exception:
                return client.reply_error(self.SYNTAX_ERROR)
            # reverse = (request[0] == b'zrevrange')
            if N == 4:
                if request[4].lower() == b'withscores':
                    result = []
                    [result.extend((v, score)) for score, v in
                     value.range(start, end, scores=True)]
                else:
                    return client.reply_error(self.SYNTAX_ERROR)
            else:
                result = list(value.range(start, end))
            client.reply_multi_bulk(result)

    @command('Sorted Sets')
    def zrangebyscore(self, client, request, N):
        check_input(request, N < 3 or N > 7)
        value = client.db.get(request[1])
        if value is None:
            client.reply_multi_bulk(())
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                minval, include_min, maxval, include_max = self._score_values(
                    request[2], request[3])
            except Exception:
                return client.reply_error(self.SYNTAX_ERROR)
            request = request[4:]
            withscores = False
            offset = 0
            count = None
            while request:
                if request[0].lower() == b'withscores':
                    withscores = True
                    request = request[1:]
                elif request[0].lower() == b'limit':
                    try:
                        offset = int(request[1])
                        count = int(request[2])
                    except Exception:
                        return client.reply_error(self.SYNTAX_ERROR)
                    request = request[3:]
                else:
                    return client.reply_error(self.SYNTAX_ERROR)
            if withscores:
                result = []
                [result.extend((v, score)) for score, v in
                 value.range_by_score(minval, maxval, scores=True,
                                      start=offset, num=count,
                                      include_min=include_min,
                                      include_max=include_max)]
            else:
                result = list(value.range_by_score(minval, maxval,
                                                   start=offset, num=count,
                                                   include_min=include_min,
                                                   include_max=include_max))
            client.reply_multi_bulk(result)

    @command('Sorted Sets')
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

    @command('Sorted Sets', True)
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
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(removed)

    @command('Sorted Sets', True)
    def zremrangebyrank(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                start, end = self._range_values(value, request[2], request[3])
            except Exception:
                return client.reply_error(self.SYNTAX_ERROR)
            removed = value.remove_range(start, end)
            if removed:
                self._signal(self.NOTIFY_ZSET, db, request[0], key, removed)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(removed)

    @command('Sorted Sets', True)
    def zremrangebyscore(self, client, request, N):
        check_input(request, N != 3)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_zero()
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            try:
                minval, include_min, maxval, include_max = self._score_values(
                    request[2], request[3])
            except Exception:
                return client.reply_error(self.SYNTAX_ERROR)
            removed = value.remove_range_by_score(minval, maxval, include_min,
                                                  include_max)
            if removed:
                self._signal(self.NOTIFY_ZSET, db, request[0], key, removed)
            if db.pop(key, value) is not None:
                self._signal(self.NOTIFY_GENERIC, db, 'del', key)
            client.reply_int(removed)

    @command('Sorted Sets', supported=False)
    def zrevrange(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Sorted Sets', supported=False)
    def zrevrangebyscore(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Sorted Sets')
    def zscore(self, client, request, N):
        check_input(request, N != 2)
        key = request[1]
        db = client.db
        value = db.get(key)
        if value is None:
            client.reply_bulk(None)
        elif not isinstance(value, self.zset_type):
            client.reply_wrongtype()
        else:
            score = value.score(request[2], None)
            if score is not None:
                score = str(score).encode('utf-8')
            client.reply_bulk(score)

    @command('Sorted Sets', True)
    def zunionstore(self, client, request, N):
        self._zsetoper(client, request, N)

    @command('Sorted Sets', supported=False)
    def zscan(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    # #########################################################################
    # #    PUBSUB COMMANDS
    @command('Pub/Sub', script=0)
    def psubscribe(self, client, request, N):
        check_input(request, not N)
        for pattern in request[1:]:
            p = self._patterns.get(pattern)
            if not p:
                pre = redis_to_py_pattern(pattern.decode('utf-8'))
                p = pubsub_patterns(re.compile(pre), set())
                self._patterns[pattern] = p
            p.clients.add(client)
            client.patterns.add(pattern)
            count = reduce(lambda x, y: x + int(client in y.clients),
                           self._patterns.values())
            client.reply_multi_bulk((b'psubscribe', pattern, count))

    @command('Pub/Sub')
    def pubsub(self, client, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'channels':
            check_input(request, N > 2)
            if N == 2:
                pre = re.compile(redis_to_py_pattern(
                    request[2].decode('utf-8')))
                channels = []
                for channel in self._channels:
                    if pre.match(channel.decode('utf-8', 'ignore')):
                        channels.append(channel)
            else:
                channels = list(self._channels)
            client.reply_multi_bulk(channels)
        elif subcommand == 'numsub':
            count = []
            for channel in request[2:]:
                clients = self._channels.get(channel, ())
                count.extend((channel, len(clients)))
            client.reply_multi_bulk(count)
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
        msg = self._parser.multi_bulk((b'message', channel, message))
        count = self._publish_clients(msg, self._channels.get(channel, ()))
        for pattern in self._patterns.values():
            g = pattern.re.match(ch)
            if g:
                count += self._publish_clients(msg, pattern.clients)
        client.reply_int(count)

    @command('Pub/Sub', script=0)
    def punsubscribe(self, client, request, N):
        patterns = request[1:] if N else list(self._patterns)
        for pattern in patterns:
            if pattern in self._patterns:
                p = self._patterns[pattern]
                if client in p.clients:
                    client.patterns.discard(pattern)
                    p.clients.remove(client)
                    if not p.clients:
                        self._patterns.pop(pattern)
                    client.reply_multi_bulk((b'punsubscribe', pattern))

    @command('Pub/Sub', script=0)
    def subscribe(self, client, request, N):
        check_input(request, not N)
        for channel in request[1:]:
            clients = self._channels.get(channel)
            if not clients:
                self._channels[channel] = clients = set()
            clients.add(client)
            client.channels.add(channel)
            client.reply_multi_bulk((b'subscribe', channel, len(clients)))

    @command('Pub/Sub', script=0)
    def unsubscribe(self, client, request, N):
        channels = request[1:] if N else list(self._channels)
        for channel in channels:
            if channel in self._channels:
                clients = self._channels[channel]
                if client in clients:
                    client.channels.discard(channel)
                    clients.remove(client)
                    if not clients:
                        self._channels.pop(channel)
                    client.reply_multi_bulk((b'unsubscribe', channel))

    # #########################################################################
    # #    TRANSACTION COMMANDS
    @command('Transactions', script=0)
    def discard(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_error("DISCARD without MULTI")
        else:
            self._close_transaction(client)
            client.reply_ok()

    @command('Transactions', name='exec', script=0)
    def execute(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_error("EXEC without MULTI")
        else:
            requests = client.transaction
            if client.flag & self.DIRTY_CAS:
                self._close_transaction(client)
                client.reply_multi_bulk(())
            else:
                self._close_transaction(client)
                client.reply_multi_bulk_len(len(requests))
                for handle, request in requests:
                    client._execute_command(handle, request)

    @command('Transactions', script=0)
    def multi(self, client, request, N):
        check_input(request, N)
        if client.transaction is None:
            client.reply_ok()
            client.transaction = []
        else:
            self.error_replay("MULTI calls can not be nested")

    @command('Transactions', script=0)
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

    @command('Transactions', script=0)
    def unwatch(self, client, request, N):
        check_input(request, N)
        transaction = client.transaction
        self._close_transaction(client)
        client.transaction = transaction
        client.reply_ok()

    # #########################################################################
    # #    SCRIPTING
    @command('Scripting', supported=False)
    def eval(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Scripting', supported=False)
    def evalsha(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Scripting', supported=False)
    def script(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    # #########################################################################
    # #    CONNECTION COMMANDS
    @command('Connections', script=0)
    def auth(self, client, request, N):
        check_input(request, N != 1)
        client.password = request[1]
        if client.password != client._producer.password:
            client.reply_error("wrong password")
        else:
            client.reply_ok()

    @command('Connections')
    def echo(self, client, request, N):
        check_input(request, N != 1)
        client.reply_bulk(request[1])

    @command('Connections')
    def ping(self, client, request, N):
        check_input(request, N)
        client.reply_status('PONG')

    @command('Connections', script=0)
    def quit(self, client, request, N):
        check_input(request, N)
        client.reply_ok()
        client.close()

    @command('Connections')
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

    # #########################################################################
    # #    SERVER COMMANDS
    @command('Server', supported=False)
    def bgrewriteaof(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server')
    def bgsave(self, client, request, N):
        check_input(request, N)
        self._save()
        client.reply_ok()

    @command('Server')
    def client(self, client, request, N):
        check_input(request, not N)
        subcommand = request[1].decode('utf-8').lower()
        if subcommand == 'list':
            check_input(request, N != 1)
            value = '\n'.join(self._client_list(client))
            client.reply_bulk(value.encode('utf-8'))
        else:
            client.reply_error("unknown command 'client %s'" % subcommand)

    @command('Server')
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

    @command('Server')
    def dbsize(self, client, request, N):
        check_input(request, N != 0)
        client.reply_int(len(client.db))

    @command('Server', supported=False, subcommands=['object', 'segfault'])
    def debug(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server', True)
    def flushdb(self, client, request, N):
        check_input(request, N)
        client.db.flush()
        client.reply_ok()

    @command('Server', True)
    def flushall(self, client, request, N):
        check_input(request, N)
        for db in self.databases.values():
            db.flush()
        client.reply_ok()

    @command('Server')
    def info(self, client, request, N):
        check_input(request, N)
        info = '\n'.join(self._flat_info())
        client.reply_bulk(info.encode('utf-8'))

    @command('Server')
    def lastsave(self, client, request, N):
        check_input(request, N)
        client.reply_int(self._last_save)

    @command('Server', script=0)
    def monitor(self, client, request, N):
        check_input(request, N)
        client.flag |= self.MONITOR
        self._monitors.add(client)
        client.reply_ok()

    @command('Server', script=0)
    def save(self, client, request, N):
        check_input(request, N)
        self._save(False)
        client.reply_ok()

    @command('Server', supported=False)
    def shutdown(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server', supported=False)
    def slaveof(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server', supported=False)
    def slowlog(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server', supported=False)
    def sync(self, client, request, N):
        client.reply_error(self.NOT_SUPPORTED)

    @command('Server')
    def time(self, client, request, N):
        check_input(request, N != 0)
        t = time.time()
        seconds = math.floor(t)
        microseconds = int(1000000*(t-seconds))
        client.reply_multi_bulk((seconds, microseconds))

    # #########################################################################
    # #    INTERNALS
    def _cron(self):
        dirty = self._dirty
        if dirty:
            now = time.time()
            gap = now - self._last_save
            for interval, changes in self.cfg.key_value_save:
                if gap >= interval and dirty >= changes:
                    self._save()
                    break
        self._loop.call_later(1, self._cron)

    def _set(self, client, key, value, seconds=0, milliseconds=0,
             nx=False, xx=False):
        try:
            seconds = int(seconds)
            milliseconds = 0.001*int(milliseconds)
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
            client.reply_multi_bulk((key, elem))
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
        key, field = request[1], request[2]
        try:
            increment = type(request[3])
        except Exception:
            return client.reply_error(
                'value is not an %s or out of range' % type.__name__)
        db = client.db
        hash = db.get(key)
        if hash is None:
            hash = self.hash_type()
            db._data[key] = hash
        elif not isinstance(hash, self.hash_type):
            return client.reply_wrongtype()
        if field in hash:
            try:
                value = type(hash[field])
            except Exception:
                return client.reply_error(
                    'hash value is not an %s' % type.__name__)
            increment += value
        hash[field] = increment
        self._signal(self.NOTIFY_HASH, db, request[0], key, 1)
        return increment

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

    def _zsetoper(self, client, request, N):
        check_input(request, N < 3)
        db = client.db
        cmnd = request[0]
        try:
            des = request[1]
            try:
                numkeys = int(request[2])
            except Exception:
                numkeys = 0
            if numkeys <= 0:
                raise ValueError('at least 1 input key is needed for '
                                 'ZUNIONSTORE/ZINTERSTORE')
            sets = []
            for key in request[3:3+numkeys]:
                value = db.get(key)
                if value is None:
                    value = self.zset_type()
                elif not isinstance(value, self.zset_type):
                    return client.reply_wrongtype()
                sets.append(value)
            if len(sets) != numkeys:
                raise ValueError('numkeys does not match number of sets')
            op = set((b'weights', b'aggregate'))
            request = request[3+numkeys:]
            weights = None
            aggregate = sum
            while request:
                name = request[0].lower()
                if name in op:
                    op.discard(name)
                    if name == b'weights':
                        weights = [float(v) for v in request[1:1+numkeys]]
                        request = request[1+numkeys:]
                    elif len(request) > 1:
                        aggregate = self.zset_aggregate.get(request[1])
                        request = request[2:]
                else:
                    raise ValueError(self.SYNTAX_ERROR)
            if not aggregate:
                raise ValueError(self.SYNTAX_ERRO)
            if weights is None:
                weights = [1]*numkeys
            elif len(weights) != numkeys:
                raise ValueError(self.SYNTAX_ERROR)
        except Exception as e:
            return client.reply_error(str(e))
        if cmnd == b'zunionstore':
            result = self.zset_type.union(sets, weights, aggregate)
        else:
            result = self.zset_type.inter(sets, weights, aggregate)
        if db.pop(des) is not None:
            self._signal(self.NOTIFY_GENERIC, db, 'del', des, 1)
        db._data[des] = result
        self._signal(self.NOTIFY_ZSET, db, cmnd, des, len(result))
        client.reply_int(len(result))

    def _score_values(self, min_value, max_value):
        include_min = include_max = True
        if min_value and min_value[0] == 40:
            include_min = False
            min_value = min_value[1:]
        if max_value and max_value[0] == 40:
            include_max = False
            max_value = max_value[1:]
        return float(min_value), include_min, float(max_value), include_max

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
        writer = self._writer
        if writer and writer.is_alive():
            self.logger.warning('Cannot save, background saving in progress')
        else:
            from multiprocessing import Process
            data = self._dbs()
            self._dirty = 0
            self._last_save = int(time.time())
            if async:
                self.logger.debug('Saving database in background process')
                self._writer = Process(target=save_data,
                                       args=(self.cfg, self._filename, data))
                self._writer.start()
            else:
                self.logger.debug('Saving database')
                save_data(self.cfg, self._filename, data)

    def _dbs(self):
        data = [(db._num, db._data) for db in self.databases.values()
                if len(db._data)]
        return (1, data)

    def _loaddb(self):
        filename = self._filename
        if self.cfg.key_value_save and os.path.isfile(filename):
            self.logger.info('loading data from "%s"', filename)
            with open(filename, 'rb') as file:
                data = pickle.load(file)
            version, dbs = data
            for num, data in dbs:
                db = self.databases.get(num)
                if db is not None:
                    db._data = data

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

    def _remove_connection(self, client, _, **kw):
        # Remove a client from the server
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

    def _write_to_monitors(self, client, request):
        # addr = '%s:%s' % self._transport.get_extra_info('addr')
        cmds = b'" "'.join(request)
        message = '+%s [0 %s] "'.encode('utf-8') + cmds + b'"\r\n'
        remove = set()
        for m in self._monitors:
            try:
                m._transport.write(message)
            except Exception:
                remove.add(m)
        if remove:
            self._monitors.difference_update(remove)


class Db:
    '''A database.
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

    # #########################################################################
    # #    INTERNALS
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
            return self._expires[key][1]
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

    def ttl(self, key, m=1):
        if key in self._expires:
            self.store._hit_keys += 1
            handle, value = self._expires[key]
            return max(0, int(m*(handle._when - self._loop.time())))
        elif key in self._data:
            self.store._hit_keys += 1
            return -1
        else:
            self.store._missed_keys += 1
            return -2

    def info(self):
        return {'Keys': len(self._data),
                'expires': len(self._expires)}

    def pop(self, key, value=None):
        if not value:
            if key in self._data:
                value = self._data.pop(key)
                return value
            elif key in self._expires:
                handle, value = self._expires.pop(key)
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
            handle, _ = self._expires.pop(key)
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
