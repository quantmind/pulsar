import time
from functools import partial

import pulsar
from pulsar.utils.structures import OrderedDict
from pulsar.utils.pep import to_string

from .parser import CommandError


COMMANDS_INFO = OrderedDict()


def check_input(request, failed):
    if failed:
        raise CommandError("wrong number of arguments for '%s'" % request[0])


class command:
    '''Decorator for pulsar-ds server commands
    '''
    def __init__(self, group, write=False, name=None,
                 script=1, supported=True, subcommands=None):
        self.group = group
        self.write = write
        self.name = name
        self.script = script
        self.supported = supported
        self.subcommands = subcommands

    @property
    def url(self):
        return 'http://redis.io/commands/%s' % self.name

    def __call__(self, f):
        self.method_name = f.__name__
        if not self.name:
            self.name = self.method_name
        COMMANDS_INFO[self.name] = self
        f._info = self
        return f


class ClientMixin:

    def __init__(self, store):
        self.store = store
        self.database = 0
        self.transaction = None
        self.last_command = ''
        self.flag = 0
        self.blocked = None

    @property
    def db(self):
        return self.store.databases[self.database]

    def execute(self, request):
        '''Execute a new ``request``.
        '''
        handle = None
        if request:
            request[0] = command = to_string(request[0]).lower()
            info = COMMANDS_INFO.get(command)
            if info:
                handle = getattr(self.store, info.method_name)
            #
            if self.channels or self.patterns:
                if command not in self.store.SUBSCRIBE_COMMANDS:
                    return self.reply_error(self.store.PUBSUB_ONLY)
            if self.blocked:
                return self.reply_error('Blocked client cannot request')
            if self.transaction is not None and command not in 'exec':
                self.transaction.append((handle, request))
                return self._transport.write(self.store.QUEUED)
        self._execute_command(handle, request)

    def _execute_command(self, handle, request):
        try:
            if request:
                command = request[0]
                if not handle:
                    self._loop.logger.info("unknown command '%s'" % command)
                    return self.reply_error("unknown command '%s'" % command)
                if self.store._password != self.password:
                    if command != 'auth':
                        return self.reply_error(
                            'Authentication required', 'NOAUTH')
                handle(self, request, len(request) - 1)
            else:
                command = ''
                return self.reply_error("no command")
        except CommandError as e:
            self.reply_error(str(e))
        except Exception:
            self._loop.logger.exception("Server error on '%s' command",
                                        command)
            self.reply_error('Server Error')
        finally:
            self.last_command = command

    def reply_ok(self):
        raise NotImplementedError

    def reply_status(self, status):
        raise NotImplementedError

    def reply_error(self, value, prefix=None):
        raise NotImplementedError

    def reply_wrongtype(self):
        raise NotImplementedError

    def reply_int(self, value):
        raise NotImplementedError

    def reply_one(self):
        raise NotImplementedError

    def reply_zero(self):
        raise NotImplementedError

    def reply_bulk(self, value):
        raise NotImplementedError

    def reply_multi_bulk(self, value):
        raise NotImplementedError

    def reply_multi_bulk_len(self, len):
        raise NotImplementedError


class PulsarStoreClient(pulsar.Protocol, ClientMixin):
    '''Used both by client and server'''

    def __init__(self, cfg, *args, **kw):
        super().__init__(*args, **kw)
        ClientMixin.__init__(self, self._producer._key_value_store)
        self.cfg = cfg
        self.parser = self._producer._parser_class()
        self.started = time.time()
        self.channels = set()
        self.patterns = set()
        self.watched_keys = None
        self.password = b''
        self.bind_event('connection_lost',
                        partial(self.store._remove_connection, self))

    # Client Mixin Implementation
    def reply_ok(self):
        self._write(self.store.OK)

    def reply_status(self, value):
        self._write(('+%s\r\n' % value).encode('utf-8'))

    def reply_int(self, value):
        self._write((':%d\r\n' % value).encode('utf-8'))

    def reply_one(self):
        self._write(self.store.ONE)

    def reply_zero(self):
        self._write(self.store.ZERO)

    def reply_error(self, value, prefix=None):
        prefix = prefix or 'ERR'
        self._write(('-%s %s\r\n' % (prefix, value)).encode('utf-8'))

    def reply_wrongtype(self):
        # Quick wrong type method
        self._write((b'-WRONGTYPE Operation against a key holding '
                     b'the wrong kind of value\r\n'))

    def reply_bulk(self, value=None):
        if value is None:
            self._write(self.store.NIL)
        else:
            self._write(self.store._parser.bulk(value))

    def reply_multi_bulk(self, value=None):
        self._write(self.store._parser.multi_bulk(value))

    def reply_multi_bulk_len(self, value):
        self._write(self.store._parser.multi_bulk_len(value))

    # Protocol Implementaton
    def data_received(self, data):
        self.parser.feed(data)
        request = self.parser.get()
        while request is not False:
            if self.store._monitors:
                self.store._write_to_monitors(self, request)
            self.execute(request)
            request = self.parser.get()

    # Internals
    def _write(self, response):
        if self.transaction is not None:
            self.transaction.append(response)
        elif not self._transport._closing:
            self._transport.write(response)


class Blocked:
    '''Handle blocked keys for a client
    '''
    def __init__(self, client, command, keys, timeout, dest=None):
        self.command = command
        self.keys = set(keys)
        self.dest = dest
        self._called = False
        db = client.db
        for key in self.keys:
            clients = db._blocking_keys.get(key)
            if clients is None:
                db._blocking_keys[key] = clients = set()
            clients.add(client)
        client.store._bpop_blocked_clients += 1
        if timeout:
            self.handle = client._loop.call_later(
                timeout, self.unblock, client)
        else:
            self.handle = None

    def unblock(self, client, key=None, value=None):
        if not self._called:
            self._called = True
            if self.handle:
                self.handle.cancel()
            store = client.store
            client.blocked = None
            store._bpop_blocked_clients -= 1
            #
            # make sure to remove the client from the set of blocked
            # clients in the database associated with key
            if key is None:
                bkeys = client.db._blocking_keys
                for key in self.keys:
                    clients = bkeys.get(key)
                    if clients:
                        clients.discard(client)
                        if not clients:
                            bkeys.pop(key)
            #
            # send the response
            if value is None:
                client._write(store.NULL_ARRAY)
            else:
                store._block_callback(client, self.command, key,
                                      value, self.dest)


def redis_to_py_pattern(pattern):
    return ''.join(_redis_to_py_pattern(pattern))


def _redis_to_py_pattern(pattern):
    clear, esc = False, False
    s, q, op, cp, e = '*', '?', '[', ']', '\\'

    for v in pattern:
        if v == s and not esc:
            yield '(.*)'
        elif v == q and not esc:
            yield '.'
        elif v == op and not esc:
            esc = True
            yield v
        elif v == cp and esc:
            esc = False
            yield v
        elif v == e:
            clear, esc = True
            yield v
        elif clear:
            clear, esc = False, False
            yield v
        else:
            yield v
    yield '$'
