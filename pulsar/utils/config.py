'''Configuration utilities. originally from gunicorn_,
adapted and modified for pulsar.

Original Gunicorn Licence

This file is part of gunicorn released under the MIT license.
See the NOTICE for more information.

.. _gunicorn: http://gunicorn.org/
'''
import copy
import inspect
import argparse
import os
import textwrap
import types

from pulsar import __version__, SERVER_NAME, parse_address
from . import system
from .httpurl import to_bytes, iteritems, HttpParser as PyHttpParser,\
                     native_str
from .importer import import_system_file


__all__ = ['Config',
           'SimpleSetting',
           'Setting',
           'defaults',
           'ordered_settings',
           'validate_string',
           'validate_callable',
           'validate_bool',
           'validate_list',
           'validate_pos_int',
           'validate_pos_float',
           'make_settings']

class DefaultSettings:

    def __init__(self):
        # port for serving a socket
        self.PORT = 8060
        # timeout for asynchronous Input/Output
        self.IO_TIMEOUT = None
        # Maximum number of concurrenct clients
        self.BACKLOG = 2048
        # Actors timeout
        self.TIMEOUT = 30
        # mailbox clients blocking
        self.message_timeout = 3

defaults = DefaultSettings()
KNOWN_SETTINGS = {}
KNOWN_SETTINGS_ORDER = []


def def_arity1(server):
    pass

def def_arity2(worker, req):
    pass

def wrap_method(func):
    def _wrapped(instance, *args, **kwargs):
        return func(*args, **kwargs)
    return _wrapped


def ordered_settings():
    for name in KNOWN_SETTINGS_ORDER:
        yield KNOWN_SETTINGS[name]

def make_settings(apps=None, include=None, exclude=None):
    '''Creates a dictionary of available settings for given
applications *apps*.

:parameter apps: Optional list of application names.
:parameter include: Optional list of settings to include.
:parameter exclude: Optional list of settings to exclude.
:rtype: dictionary of :class:`pulsar.Setting` instances.'''
    settings = {}
    include = set(include or ())
    exclude = set(exclude or ())
    apps = set(apps or ())
    for s in ordered_settings():
        setting = s()
        if setting.name not in include:
            if setting.name in exclude:
                continue    # setting name in exclude set
            if setting.app and setting.app not in apps:
                continue    # the setting is for an app not in the apps set
        settings[setting.name] = setting.copy()
    return settings


class Config(object):
    '''Dictionary containing :class:`Setting` parameters for
fine tuning pulsar servers. It provides easy access to :attr:`Setting.value`
attribute by exposing the :attr:`Setting.name` as attribute.

.. attribute:: settings

    Dictionary of all :class:`Settings` instances available. The
    keys are given by the :attr:`Setting.name` attribute.
'''
    exclude_from_config = set(('config',))
    
    def __init__(self, description=None, epilog=None,
                 version=None, app=None, include=None,
                 exclude=None, settings=None):
        if settings is None:
            settings = make_settings(app, include, exclude)
        self.settings = settings
        self.description = description or 'Pulsar server'
        self.epilog = epilog or 'Have fun!'
        self.version = version or __version__
        
    def __iter__(self):
        return iter(self.settings)
    
    def __len__(self):
        return len(self.settings)
    
    def items(self):
        for k, setting in iteritems(self.settings):
            yield k, setting.value
    
    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        for k,v in state.items():
            self.__dict__[k] = v

    def __getattr__(self, name):
        return self.get(name)

    def __setattr__(self, name, value):
        if name != "settings" and name in self.settings:
            raise AttributeError("Invalid access!")
        super(Config, self).__setattr__(name, value)

    def get(self, name, default=None):
        if name not in self.settings:
            if name in KNOWN_SETTINGS:
                return default
            raise AttributeError("No configuration setting for: %s" % name)
        return self.settings[name].get()

    def set(self, name, value):
        if name not in self.settings:
            raise AttributeError("No configuration setting for: %s" % name)
        self.settings[name].set(value)

    def parser(self):
        '''Create the argparser_ for this configuration by adding all
settings via the :meth:`Setting.add_argument`.

:rtype: an instance of :class:`argparse.ArgumentParser`.

.. _argparser: http://docs.python.org/dev/library/argparse.html
'''
        kwargs = {
            "description": self.description,
            "epilog": self.epilog
        }
        parser = argparse.ArgumentParser(**kwargs)
        parser.add_argument('--version',
                            action='version',
                            version=self.version)
        setts = self.settings
        sorter = lambda x: (setts[x].section, setts[x].order)
        for k in sorted(setts, key=sorter):
            setts[k].add_argument(parser)
        return parser

    def import_from_module(self, mod=None):
        if mod:
            self.set('config', mod)
        try:
            mod = import_system_file(self.config)
        except Exception as e:
            raise RuntimeError('Failed to read config file "%s". %s' %\
                               (self.config, e))
        unknowns = []
        if mod:
            for k in dir(mod):
                # Skip private functions and attributes
                kl = k.lower()
                if k.startswith('_') or kl in self.exclude_from_config:
                    continue
                val = getattr(mod, k)
                # add unknown names to list
                if kl not in self.settings:
                    unknowns.append((k, val))
                else:
                    self.set(kl, val)
        return unknowns
        
    def on_start(self):
        '''Invoked by a :class:`pulsar.Application` just before starting.'''
        for sett in self.settings.values():
            sett.on_start()

    @property
    def workers(self):
        return self.settings['workers'].get()

    @property
    def address(self):
        bind = self.settings['bind']
        if bind:
            return parse_address(to_bytes(bind.get()))

    @property
    def uid(self):
        user = self.settings.get('user')
        if user:
            return system.get_uid(user.get())

    @property
    def gid(self):
        group = self.settings.get('group')
        if group:
            return system.get_gid(group.get())

    @property
    def proc_name(self):
        pn = self.settings.get('process_name')
        if pn:
            pn = pn.get()
        if pn is not None:
            return pn
        else:
            pn = self.settings.get('default_process_name')
            if pn:
                return pn.get()
        
    def copy(self):
        cls = self.__class__
        me = cls.__new__(cls)
        for name, value in iteritems(self.__dict__):
            if name == 'settings':
                value = copy.deepcopy(value)
            me.__dict__[name] = value
        return me        
    
    def __copy__(self):
        return self.copy()
    
    def __deepcopy__(self, memo):
        return self.__copy__()
    
    def new_config(self):
        cfg = self.__class__()
        for key, sett in iteritems(self.settings):
            if key in cfg.settings and sett.inherit:
                cfg.set(key, sett.value)
        return cfg
            

class SettingMeta(type):
    '''A metaclass which collects all setting classes and put them
in the global ``KNOWN_SETTINGS`` list.'''
    def __new__(cls, name, bases, attrs):
        super_new = super(SettingMeta, cls).__new__
        parents = [b for b in bases if isinstance(b, SettingMeta)]
        val = attrs.get("validator")
        attrs["validator"] = wrap_method(val) if val else None
        if attrs.pop('virtual', False):
            return super_new(cls, name, bases, attrs)
        attrs["order"] = len(KNOWN_SETTINGS) + 1
        new_class = super_new(cls, name, bases, attrs)
        # build one instance to increase count
        new_class()
        new_class.fmt_desc(attrs['desc'] or '')
        if not new_class.name:
            new_class.name = new_class.__name__.lower()
        if new_class.name in KNOWN_SETTINGS_ORDER:
            old_class = KNOWN_SETTINGS.pop(new_class.name)
            new_class.order = old_class.order
        else:
            KNOWN_SETTINGS_ORDER.append(new_class.name)
        KNOWN_SETTINGS[new_class.name] = new_class
        return new_class

    def fmt_desc(cls, desc):
        desc = textwrap.dedent(desc).strip()
        setattr(cls, "desc", desc)
        lines = desc.splitlines()
        setattr(cls, "short", '' if not lines else lines[0])


class SettingBase(object):
    inherit = True
    name = None
    '''The unique name used to access this setting.'''
    validator = None
    '''Validator for this setting. It provided it must be a fucntion
accepting one positional argument, the value to validate.'''
    value = None
    '''The actual value for this setting.'''
    def __str__(self):
        return '{0} ({1})'.format(self.name, self.value)
    __repr__ = __str__

    def set(self, val):
        '''Set *val* as the value in this :class:`Setting`.'''
        if hasattr(self.validator,'__call__'):
            val = self.validator(val)
        self.value = val

    def get(self):
        return self.value

    def on_start(self):
        '''Called when pulsar server starts. It can be used to perform
custom initialization for this :class:`Setting`.'''
        pass


class SimpleSetting(SettingBase):

    def __init__(self, name, value):
        self.name = name
        self.value = value


# This works for Python 2 and Python 3
class Setting(SettingMeta('BaseSettings', (SettingBase,), {'virtual': True})):
    '''A configuration parameter for pulsar. Parameters can be specified
on the command line or on a config file.'''
    creation_count = 0
    virtual = True
    '''If set to ``True`` the settings won't be loaded and it can be only used
as base class for other settings.'''
    nargs = None
    '''For positional arguments. Same usage as argparse.'''
    app = None
    '''Setting for a specific :class:`Application`.'''
    section = None
    '''Setting section, used for creating documentation.'''
    flags = None
    '''List of options strings, e.g. ``[-f, --foo]``.'''
    choices = None
    '''Restrict the argument to the choices provided.'''
    type = None
    meta = None
    action = None
    default = None
    '''Default value'''
    short = None
    desc = None

    def __init__(self, name=None, flags=None, action=None, type=None,
                 default=None, nargs=None, desc=None, validator=None):
        self.default = default if default is not None else self.default
        if self.default is not None:
            self.set(self.default)
        self.name = name or self.name
        self.flags = flags or self.flags
        self.action = action or self.action
        self.nargs = nargs or self.nargs
        self.type = type or self.type
        self.desc = desc or self.desc
        self.short = self.short or self.desc
        self.desc = self.desc or self.short
        #if validator:
        #    self.validator = wrap_method(validator)
        if self.app and not self.section:
            self.section = self.app
        if not self.section:
            self.section = 'unknown'
        self.__class__.creation_count += 1
        if not hasattr(self, 'order'):
            self.order = 1000 + self.__class__.creation_count

    def __getstate__(self):
        return self.__dict__.copy()

    def add_argument(self, parser):
        '''Add itself to the argparser.'''
        kwargs = {'nargs':self.nargs}
        if self.type and self.type != 'string':
            kwargs["type"] = self.type
        if self.flags:
            args = tuple(self.flags)
            kwargs.update({"dest": self.name,
                           "action": self.action or "store",
                           "default": None,
                           "help": "%s [%s]" % (self.short, self.default)})
            if kwargs["action"] != "store":
                kwargs.pop("type",None)
                kwargs.pop("nargs",None)
        elif self.nargs and self.name:
            args = (self.name,)
            kwargs.update({'metavar': self.meta or None,
                           'help': self.short})
        else:
            # Not added to argparser
            return
        parser.add_argument(*args, **kwargs)

    def copy(self):
        return copy.copy(self)


def validate_bool(val):
    if isinstance(val, bool):
        return val
    val = native_str(val)
    if not isinstance(val, str):
        raise TypeError("Invalid type for casting: %s" % val)
    if val.lower().strip() == "true":
        return True
    elif val.lower().strip() == "false":
        return False
    else:
        raise ValueError("Invalid boolean: %s" % val)

def validate_pos_int(val):
    if not isinstance(val, int):
        val = int(val, 0)
    else:
        # Booleans are ints!
        val = int(val)
    if val < 0:
        raise ValueError("Value must be positive: %s" % val)
    return val

def validate_pos_float(val):
    val = float(val)
    if val < 0:
        raise ValueError("Value must be positive: %s" % val)
    return val

def validate_string(val):
    va = native_str(val)
    if va is None:
        return None
    if not isinstance(va, str):
        raise TypeError("Not a string: %s" % val)
    return va.strip()

def validate_list(val):
    if val and not isinstance(val, (list, tuple)):
        raise TypeError("Not a list: %s" % val)
    return list(val)

def validate_dict(val):
    if val and not isinstance(val, dict):
        raise TypeError("Not a dictionary: %s" % val)
    return val

def validate_callable(arity):
    def _validate_callable(val):
        if not hasattr(val,'__call__'):
            raise TypeError("Value is not callable: %s" % val)
        if not inspect.isfunction(val):
            cval = val.__call__
            discount = 1
        else:
            discount = 0
            cval = val
        if arity != len(inspect.getargspec(cval)[0]) - discount:
            raise TypeError("Value must have an arity of: %s" % arity)
        return val
    return _validate_callable


class ConfigFile(Setting):
    name = "config"
    section = "Config File"
    flags = ["-c", "--config"]
    meta = "FILE"
    validator = validate_string
    default = 'config.py'
    desc = """\
        The path to a Pulsar config file.

        Only has an effect when specified on the command line or as part of an
        application specific configuration.
        """


class Workers(Setting):
    inherit = False     # not ineritable by the arbiter
    name = "workers"
    section = "Worker Processes"
    flags = ["-w", "--workers"]
    validator = validate_pos_int
    type = int
    default = 1
    desc = """\
        The number of workers for handling requests.

If you are using a multi-process concurrency, a number in the
the 2-4 x $(NUM_CORES) range should be good. If you are using threads this
number can be higher."""


class Concurrency(Setting):
    name = "concurrency"
    section = "Worker Processes"
    flags = ["--concurrency"]
    default = "process"
    desc = """\
        The type of concurrency to use: ``process`` or ``thread``.
        """


class MaxRequests(Setting):
    inherit = False     # not ineritable by the arbiter
    name = "max_requests"
    section = "Worker Processes"
    flags = ["--max-requests"]
    validator = validate_pos_int
    type = int
    default = 0
    desc = """\
        The maximum number of requests a worker will process before restarting.

        Any value greater than zero will limit the number of requests a work
        will process before automatically restarting. This is a simple method
        to help limit the damage of memory leaks.

        If this is set to zero (the default) then the automatic worker
        restarts are disabled.
        """

class Backlog(Setting):
    inherit = False     # not ineritable by the arbiter
    name = "backlog"
    section = "Worker Processes"
    flags = ["--backlog"]
    validator = validate_pos_int
    type = int
    default = 2048
    desc = """\
        The maximum number of concurrent requests.
        This refers to the number of clients that can be waiting to be served.
        Exceeding this number results in the client getting an error when
        attempting to connect. It should only affect servers under significant
        load.

        Must be a positive integer. Generally set in the 64-2048 range.
        """


class Timeout(Setting):
    inherit = False     # not ineritable by the arbiter
    name = "timeout"
    section = "Worker Processes"
    flags = ["-t", "--timeout"]
    validator = validate_pos_int
    type = int
    default = 30
    desc = """\
        Workers silent for more than this many seconds are killed and restarted.

        Generally set to thirty seconds. Only set this noticeably higher if
        you're sure of the repercussions for sync workers. For the non sync
        workers it just means that the worker process is still communicating and
        is not tied to the length of time required to handle a single request.
        """


class HttpProxyServer(Setting):
    name = "http_proxy"
    section = "Http"
    flags = ["--http-proxy"]
    default = ''
    desc = """\
        The HTTP proxy server to use with HttpClient.
        """
    def on_start(self):
        if self.value:
            os.environ['http_proxy'] = self.value
            os.environ['https_proxy'] = self.value


class HttpParser(Setting):
    name = "http_py_parser"
    section = "Http"
    flags = ["--http-py-parser"]
    action = "store_true"
    default = False
    desc = """\
    Set the python parser as default HTTP parser.
        """

    def on_start(self):
        if self.value:
            from pulsar.lib import setDefaultHttpParser 
            setDefaultHttpParser(PyHttpParser)


class Debug(Setting):
    name = "debug"
    section = "Debugging"
    flags = ["--debug"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Turn on debugging in the server.

        This limits the number of worker processes to 1 and changes some error
        handling that's sent to clients.
        """


class Daemon(Setting):
    name = "daemon"
    section = "Server Mechanics"
    flags = ["-D", "--daemon"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Daemonize the Pulsar process (posix only).

        Detaches the server from the controlling terminal and enters the
        background.
        """


class Pidfile(Setting):
    name = "pidfile"
    section = "Server Mechanics"
    flags = ["-p", "--pid"]
    meta = "FILE"
    validator = validate_string
    default = None
    desc = """\
        A filename to use for the PID file.

        If not set, no PID file will be written.
        """


class Password(Setting):
    name = "password"
    section = "Server Mechanics"
    flags = ["--password"]
    validator = validate_string
    default = None
    desc = """Set a password for the server"""


class User(Setting):
    name = "user"
    section = "Server Mechanics"
    flags = ["-u", "--user"]
    meta = "USER"
    validator = validate_string
    default = None
    desc = """\
        Switch worker processes to run as this user.

        A valid user id (as an integer) or the name of a user that can be
        retrieved with a call to pwd.getpwnam(value) or None to not change
        the worker process user.
        """


class Group(Setting):
    name = "group"
    section = "Server Mechanics"
    flags = ["-g", "--group"]
    meta = "GROUP"
    validator = validate_string
    default = None
    desc = """\
        Switch worker process to run as this group.

        A valid group id (as an integer) or the name of a user that can be
        retrieved with a call to pwd.getgrnam(value) or None to not change
        the worker processes group.
        """


class Loglevel(Setting):
    name = "loglevel"
    section = "Logging"
    flags = ["--log-level"]
    meta = "LEVEL"
    validator = validate_string
    default = "info"
    desc = """The granularity of log outputs.
            
            Valid level names are:
            
             * debug
             * info
             * warning
             * error
             * critical
             """

class LogHandlers(Setting):
    name = "loghandlers"
    section = "Logging"
    flags = ["--log-handlers"]
    default = ['console']
    validator = validate_list
    desc = """log handlers for pulsar server"""
    
    
class LogEvery(Setting):
    name = "logevery"
    section = "Logging"
    flags = ["--log-every"]
    validator = validate_pos_int
    default = 0
    desc = """Log information every n seconds"""


class LogConfig(Setting):
    name = "logconfig"
    section = "Logging"
    default = {}
    validator = validate_dict
    desc = '''
    The logging configuration dictionary.

    This settings can only be specified on a config file
    '''


class Procname(Setting):
    name = "process_name"
    section = "Process Naming"
    flags = ["-n", "--name"]
    meta = "STRING"
    validator = validate_string
    default = None
    desc = """\
        A base to use with setproctitle for process naming.

        This affects things like ``ps`` and ``top``. If you're going to be
        running more than one instance of Pulsar you'll probably want to set a
        name to tell them apart. This requires that you install the setproctitle
        module.

        It defaults to 'pulsar'.
        """


class DefaultProcName(Setting):
    name = "default_process_name"
    section = "Process Naming"
    validator = validate_string
    default = SERVER_NAME
    desc = """\
        Internal setting that is adjusted for each type of application.
        """


class WhenReady(Setting):
    name = "when_ready"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """\
        Called just after the server is started.

        The callable needs to accept a single instance variable for the Arbiter.
        """


class Prefork(Setting):
    name = "pre_fork"
    section = "Server Hooks"
    validator = validate_callable(1)
    default = staticmethod(def_arity1)
    type = "callable"
    desc = """\
        Called just before a worker is forked.

        The callable needs to accept two instance variables for the Arbiter and
        new Worker.
        """


class Postfork(Setting):
    name = "post_fork"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """\
        Called just after a worker has been forked.

        The callable needs to accept two instance variables for the Arbiter and
        new Worker.
        """


class PreExec(Setting):
    name = "pre_exec"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """\
        Called just before a new master process is forked.

        The callable needs to accept a single instance variable for the Arbiter.
        """


class PreRequest(Setting):
    name = "pre_request"
    section = "Server Hooks"
    validator = validate_callable(2)
    type = "callable"
    default = staticmethod(def_arity2)
    desc = """\
        Called just before a worker processes the request.

        The callable needs to accept two instance variables for the Worker and
        the Request.
        """


class PostRequest(Setting):
    name = "post_request"
    section = "Server Hooks"
    validator = validate_callable(2)
    type = "callable"
    default = staticmethod(def_arity2)
    desc = """\
        Called after a worker processes the request.

        The callable needs to accept two instance variables for the Worker and
        the Request.
        """


class WorkerExit(Setting):
    name = "worker_exit"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """Called just after a worker has been exited.

        The callable needs to accept one variable for the
        the just-exited Worker.
        """


class WorkerTask(Setting):
    name = "worker_task"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """Called at every event loop by the worker.

        The callable needs to accept one variable for the Worker.
        """
        
        
class ArbiterTask(Setting):
    name = "arbiter_task"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_arity1)
    desc = """Called at every event loop by the arbiter.

        The callable needs to accept one variable for the Arbiter.
        """