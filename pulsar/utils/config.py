'''Configuration utilities which provides pulsar with configuration parameters
which can be parsed from the command line. Parsing is implemented using
the python argparser_ standard library module.

Config
~~~~~~~~~~

.. autoclass:: Config
   :members:
   :member-order: bysource

SettingBase
~~~~~~~~~~~~~~

.. autoclass:: SettingBase
   :members:
   :member-order: bysource
      
Setting
~~~~~~~~~~

.. autoclass:: Setting
   :members:
   :member-order: bysource


.. _argparser: http://docs.python.org/dev/library/argparse.html
'''
import copy
import inspect
import argparse
import os
import textwrap
import types
import logging

from pulsar import __version__, SERVER_NAME, parse_address
from . import system
from .httpurl import to_bytes, iteritems, HttpParser as PyHttpParser,\
                     native_str
from .importer import import_system_file


__all__ = ['Config',
           'SimpleSetting',
           'Setting',
           #'defaults',
           'ordered_settings',
           'validate_string',
           'validate_callable',
           'validate_bool',
           'validate_list',
           'validate_pos_int',
           'validate_pos_float',
           'make_optparse_options']

LOGGER = logging.getLogger('pulsar.config')

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
section_docs = {}
KNOWN_SETTINGS = {}
KNOWN_SETTINGS_ORDER = []


def pass_through(arg):
    '''A dummy function accepting one parameter only.
    
It does nothing and it is used as default by
:ref:`Application Hooks <setting-section-application-hooks>`.'''
    pass

def wrap_method(func):
    def _wrapped(instance, *args, **kwargs):
        return func(*args, **kwargs)
    return _wrapped

def ordered_settings():
    for name in KNOWN_SETTINGS_ORDER:
        yield KNOWN_SETTINGS[name]


class Config(object):
    '''A dictionary-like container of :class:`Setting` parameters for
fine tuning pulsar servers. It provides easy access to :attr:`Setting.value`
attribute by exposing the :attr:`Setting.name` as attribute.

:param description: description used when parsing the command line, same usage
    as in the :class:`argparse.ArgumentParser` class.
:param epilog: epilog used when parsing the command line, same usage
    as in the :class:`argparse.ArgumentParser` class.
:param version: version used when parsing the command line, same usage
    as in the :class:`argparse.ArgumentParser` class.
:param apps: list of application namespaces to include in the :attr:`settings`
    attribute.

.. attribute:: settings

    Dictionary of all :class:`Setting` instances available in this
    :class:`Config` container. Keys are given by the :attr:`SettingBase.name`
    attribute.
    
.. attribute:: params

    Dictionary of additional parameters which cannot be parsed in the
    command line.
'''
    exclude_from_config = set(('config',))
    
    def __init__(self, description=None, epilog=None,
                 version=None, apps=None, include=None,
                 exclude=None, settings=None, prefix=None,
                 name=None, **params):
        if settings is None:
            self.settings = {}
        else:
            self.settings = settings
        self.name = name
        self.prefix = prefix
        self.include = set(include or ())
        self.exclude = set(exclude or ())
        self.apps = set(apps or ())
        if settings is None:
            self.update_settings()
        self.params = {}
        self.description = description or 'Pulsar server'
        self.epilog = epilog or 'Have fun!'
        self.version = version or __version__
        self.update(params)
        
    def __iter__(self):
        return iter(self.settings)
    
    def __len__(self):
        return len(self.settings)
    
    def __contains__(self, name):
        return name in self.settings
    
    def items(self):
        for k, setting in iteritems(self.settings):
            yield k, setting.value
    
    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        for k, v in state.items():
            self.__dict__[k] = v
        config = getattr(self, 'config', None)
        if config:
            self.import_from_module(config)

    def __getattr__(self, name):
        try:
            return self._get(name)
        except KeyError:
            raise AttributeError("'%s' object has no attribute '%s'." %
                                 (self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name != "settings" and name in self.settings:
            raise AttributeError("Invalid access!")
        super(Config, self).__setattr__(name, value)
    
    def update(self, data):
        '''Update this :attr:`Config` with ``data`` which is either an
instance of Mapping or :class:`Config`.'''
        for name, value in data.items():
            if value is not None:
                self.set(name, value)
            
    def get(self, name, default=None):
        '''Get the value at ``name`` for this :class:`Config` container
following this algorithm:

* returns the ``name`` value in the :attr:`settings` dictionary if available.
* returns the ``name`` value in the :attr:`params` dictionary if available.
* returns the ``default`` value.
'''
        try:
            return self._get(name, default)
        except KeyError:
            return default

    def set(self, name, value, default=False):
        '''Set the configuration :class:`Setting` at *name* with a new
*value*. If the *name* is not in this container, an :class:`AttributeError`
is raised. If ``default`` is ``True``, the :attr:`Setting.default` is
also set.'''
        if name not in self.settings:
            if self.prefix:
                prefix_name = '%s_%s' % (self.prefix, name)
                if prefix_name in self.settings:
                    return # dont set this value
            self.params[name] = value
        else:
            self.settings[name].set(value, default=default)

    def parser(self):
        '''Create the argparser_ for this configuration by adding all
settings via the :meth:`Setting.add_argument` method.

:rtype: an instance of :class:`argparse.ArgumentParser`.
'''
        kwargs = {
            "description": self.description,
            "epilog": self.epilog
        }
        parser = argparse.ArgumentParser(**kwargs)
        parser.add_argument('--version',
                            action='version',
                            version=self.version)
        return self.add_to_parser(parser)

    def add_to_parser(self, parser):
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
        '''An address to bind to, only available if a
:ref:`bind <setting-bind>` setting has been added to this :class:`Config`
container.'''
        bind = self.settings.get('bind')
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
        
    def copy(self, name=None, prefix=None):
        '''A deep copy of this :class:`Config` container. If ``prefix``
is given, it prefixes all non
:ref:`global settings <setting-section-global-server-settings>` with it.'''
        cls = self.__class__
        me = cls.__new__(cls)
        me.__dict__.update(self.__dict__)
        if prefix:
            me.prefix = prefix
        # Important, don't use the prefix from me.prefix!
        #        prefix = me.prefix
        settings = me.settings
        me.settings = {}
        for setting in settings.values():
            setting = setting.copy(name, prefix)
            me.settings[setting.name] = setting
        me.params = me.params.copy()
        return me
    
    def __copy__(self):
        return self.copy()
    
    def __deepcopy__(self, memo):
        return self.__copy__()
    
    ############################################################################
    ##    INTERNALS
    def update_settings(self):
        for s in ordered_settings():
            setting = s().copy(name=self.name, prefix=self.prefix)
            if setting.name in self.settings:  
                continue
            if setting.name not in self.include:
                if setting.name in self.exclude:
                    continue    # setting name in exclude set
                if setting.app and setting.app not in self.apps:
                    continue    # the setting is for an app not in the apps set
            self.settings[setting.name] = setting
            
    def _get(self, name, default=None):
        if name not in self.settings:
            if name in KNOWN_SETTINGS:
                return default
            if name in self.params:
                return self.params[name]
            raise KeyError("'%s'" % name)
        return self.settings[name].get()
    
            

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
        lines = desc.split('\n\n')
        setattr(cls, "short", '' if not lines else lines[0])


class SettingBase(object):
    '''This is the base class of :class:`Settings` and :class:`SimpleSetting`.
It defines attributes for a given setting parameter.'''
    name = None
    '''The unique name used to access this setting in the :class:`Config`
    container.'''
    validator = None
    '''A validating function for this setting. It provided it must be a
function accepting one positional argument, the value to validate.'''
    value = None
    '''The actual value for this setting.'''
    default = None
    '''The default value for this setting.'''
    def __str__(self):
        return '{0} ({1})'.format(self.name, self.value)
    __repr__ = __str__

    def set(self, val, default=False):
        '''Set *val* as the :attr:`value` for this :class:`SettingBase`.
If *default* is ``True`` set also the :attr:`default` value.'''
        if hasattr(self.validator, '__call__'):
            val = self.validator(val)
        self.value = val
        if default:
            self.default = val

    def get(self):
        '''Returns :attr:`value`'''
        return self.value

    def on_start(self):
        '''Called when pulsar server starts. It can be used to perform
custom initialization for this :class:`Setting`.'''
        pass
    
    def copy(self, name=None, prefix=None):
        '''Copy this :class:`SettingBase`'''
        setting = copy.copy(self)
        if prefix and not setting.is_global:
            flags = setting.flags
            if flags and flags[-1].startswith('--'):
                # Prefix a setting
                setting.orig_name = setting.name 
                setting.name = '%s_%s' % (prefix, setting.name)
                setting.flags = ['--%s-%s' % (prefix, flags[-1][2:])]
        if name and not setting.is_global:
            setting.short = '%s application. %s' % (name , setting.short)
        return setting
    

class SimpleSetting(SettingBase):

    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.orig_name = self.name


# This works for Python 2 and Python 3
class Setting(SettingMeta('BaseSettings', (SettingBase,), {'virtual': True})):
    '''A configuration parameter for pulsar. Parameters can be specified
on the command line or on a config file.'''
    creation_count = 0
    virtual = True
    '''If set to ``True`` the settings won't be loaded and it can be only used
as base class for other settings.'''
    nargs = None
    '''For positional arguments. Same usage as python :mod:`argparse` module.'''
    app = None
    '''Setting for a specific :class:`Application`.'''
    section = None
    '''Setting section, used for creating the
    :ref:`settings documentation <settings>`.'''
    flags = None
    '''List of options strings, e.g. ``[-f, --foo]``.'''
    choices = None
    '''Restrict the argument to the choices provided.'''
    type = None
    meta = None
    '''Same usage as ``metavar`` in the python :mod:`argparse` module. It is
    the name for the argument in usage message.'''
    action = None
    '''Same usage as ``action`` in the python
    :mod:`argparse.ArgumentParser.add_argument` method.'''
    short = None
    desc = None
    '''Description string'''
    is_global = False
    '''Flag used by pulsar :ref:`application framework <apps-framework>`
    when multiple application are used in a running server. If ``False``
    additional settings will be added to the :class:`Config` container.
    These settings are clones of this :class:`Setting` with
    name given by each application name and underacsore ``_``
    prefixing this :attr:`name`.'''
    orig_name = None

    def __init__(self, name=None, flags=None, action=None, type=None,
                 default=None, nargs=None, desc=None, validator=None,
                 app=None, meta=None, choices=None):
        self.app = app or self.app
        self.choices = choices or self.choices
        self.default = default if default is not None else self.default
        self.desc = desc or self.desc
        self.flags = flags or self.flags
        self.action = action or self.action
        self.meta = meta or self.meta
        self.name = name or self.name
        self.nargs = nargs or self.nargs
        self.type = type or self.type
        self.short = self.short or self.desc
        self.desc = self.desc or self.short
        if self.default is not None:
            self.set(self.default)
        if self.app and not self.section:
            self.section = self.app
        if not self.section:
            self.section = 'unknown'
        self.__class__.creation_count += 1
        if not hasattr(self, 'order'):
            self.order = 1000 + self.__class__.creation_count

    def __getstate__(self):
        return self.__dict__.copy()

    def add_argument(self, parser, set_default=False):
        '''Add this :class:`Setting` to the *parser*, an instance of
python :class:`argparse.ArgumentParser`, only if :attr:`flags` or
:attr:`nargs` and :attr:`name` are defined.'''
        default = self.default if set_default else None
        kwargs = {'nargs':self.nargs}
        if self.type and self.type != 'string':
            kwargs["type"] = self.type
        if self.choices:
            kwargs["choices"] = self.choices
        if self.flags:
            args = tuple(self.flags)
            kwargs.update({"dest": self.name,
                           "action": self.action or "store",
                           "default": default,
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


def make_optparse_options(apps=None, exclude=None, include=None): # pragma nocover
    '''Create a tuple of optparse options'''
    from optparse import make_option
    class AddOptParser(list):
        def add_argument(self, *args, **kwargs):
            self.append(make_option(*args, **kwargs))
    config = Config(apps=apps, exclude=None, include=None)
    parser = AddOptParser()
    config.add_to_parser(parser)
    return tuple(parser)
    

################################################################################
##    Global Server Settings

section_docs['Global Server Settings'] = '''
These settings are global in the sense that they are used by the arbiter
as well as all pulsar workers. They are server configuration parameters.
'''
class Global(Setting):
    virtual = True
    section = "Global Server Settings"
    is_global = True


class ConfigFile(Global):
    name = "config"
    flags = ["-c", "--config"]
    meta = "FILE"
    validator = validate_string
    default = 'config.py'
    desc = """\
        The path to a Pulsar config file, where default Settings
        paramaters can be specified.
        """

    
class HttpProxyServer(Global):
    name = "http_proxy"
    flags = ["--http-proxy"]
    default = ''
    desc = """\
        The HTTP proxy server to use with HttpClient.
        """
    def on_start(self):
        if self.value:
            os.environ['http_proxy'] = self.value
            os.environ['https_proxy'] = self.value


class HttpParser(Global):
    name = "http_py_parser"
    flags = ["--http-py-parser"]
    action = "store_true"
    default = False
    desc = """\
    Set the python parser as default HTTP parser.
        """

    def on_start(self):
        if self.value:
            from pulsar.utils.httpurl import setDefaultHttpParser 
            setDefaultHttpParser(PyHttpParser)


class Debug(Global):
    name = "debug"
    flags = ["--debug"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Turn on debugging in the server.

        This limits the number of worker processes to 1 and changes some error
        handling that's sent to clients.
        """


class Daemon(Global):
    name = "daemon"
    flags = ["-D", "--daemon"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Daemonize the Pulsar process (posix only).

        Detaches the server from the controlling terminal and enters the
        background.
        """


class Pidfile(Global):
    name = "pidfile"
    flags = ["-p", "--pid"]
    meta = "FILE"
    validator = validate_string
    default = None
    desc = """\
        A filename to use for the PID file.

        If not set, no PID file will be written.
        """


class Password(Global):
    name = "password"
    flags = ["--password"]
    validator = validate_string
    default = None
    desc = """Set a password for the server"""


class User(Global):
    name = "user"
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


class Group(Global):
    name = "group"
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


class Loglevel(Global):
    name = "loglevel"
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

class LogHandlers(Global):
    name = "loghandlers"
    flags = ["--log-handlers"]
    default = ['console']
    validator = validate_list
    desc = """log handlers for pulsar server"""


class LogConfig(Global):
    name = "logconfig"
    default = {}
    validator = validate_dict
    desc = '''
    The logging configuration dictionary.

    This settings can only be specified on a config file
    '''


class Procname(Global):
    name = "process_name"
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


class DefaultProcName(Global):
    name = "default_process_name"
    validator = validate_string
    default = SERVER_NAME
    desc = """\
        Internal setting that is adjusted for each type of application.
        """

################################################################################
##    Worker Processes

section_docs['Worker Processes'] = '''
This group of configuration parameters control the number of actors
for a given :class:`pulsar.Monitor`, the type of concurreny of the server and
other actor-specific parameters.
'''
    
class Workers(Setting):
    name = "workers"
    section = "Worker Processes"
    flags = ["-w", "--workers"]
    validator = validate_pos_int
    type = int
    default = 1
    desc = """\
        The number of workers for handling requests.
        
If using a multi-process concurrency, a number in the
the ``2-4 x NUM_CORES`` range should be good. If you are using threads this
number can be higher."""


class Concurrency(Setting):
    inherit = True # Inherited by the arbiter if an application is first created
    name = "concurrency"
    section = "Worker Processes"
    flags = ["--concurrency"]
    default = "process"
    desc = """\
        The type of concurrency to use: ``process`` or ``thread``.
        """


class MaxRequests(Setting):
    name = "max_requests"
    section = "Worker Processes"
    flags = ["--max-requests"]
    validator = validate_pos_int
    type = int
    default = 0
    desc = """\
        The maximum number of requests a worker will process before restarting.

        Any value greater than zero will limit the number of requests a worker
        will process before automatically restarting. This is a simple method
        to help limit the damage of memory leaks.

        If this is set to zero (the default) then the automatic worker
        restarts are disabled.
        """

class Timeout(Setting):
    name = "timeout"
    section = "Worker Processes"
    flags = ["-t", "--timeout"]
    validator = validate_pos_int
    type = int
    default = 30
    desc = """\
        Workers silent for more than this many seconds are
        killed and restarted."""


class ThreadWorkers(Setting):
    name = "thread_workers"
    section = "Worker Processes"
    flags = ["--thread-workers"]
    validator = validate_pos_int
    type = int
    default = 1
    desc = """\
        The number of threads in an actor thread pool.
        
        The thread pool is used by actors to perform CPU intensive calculations.
        In this way the actor main thread is free to listen to events on file
        descriptors and process them as quick as possible."""


################################################################################
##    APPLICATION HOOKS

section_docs['Application Hooks'] = '''
Application hooks are functions which can be specified in a
:ref:`config <setting-config>` file to perform custom tasks in a pulsar server.
These tasks can be scheduled when events occurs or at every event loop of
the various components of a pulsar application.

All application hooks are functions which accept one parameter only, the actor
invoking the function.'''
    
class Postfork(Setting):
    name = "post_fork"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called just after a worker has been forked.

        The event loop is not yet available.
        """
        

class WhenReady(Setting):
    name = "when_ready"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called just before a worker starts its event loop.
        
        This is a chance to setup :class:`pulsar.EventLoop` callbacks which
        can run periodically, at every loop or when some defined events occur.
        """
        
        
class WhenExit(Setting):
    name = "when_exit"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called just before an actor is garbadge collected.
        
        This is a chance to check the actor status if needed.
        """


class ConnectionMade(Setting):
    name = "connection_made"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called after a new connection is made.

        The callable needs to accept one instance variables for the
        connection instance.
        """


class ConnectionLost(Setting):
    name = "connection_lost"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """
        Called after a connection is lost.

        The callable needs to accept one instance variables for the
        connection instance.
        """


class PreRequest(Setting):
    name = "pre_request"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called just before a worker processes the request.

        The callable needs to accept two instance variables for the Worker and
        the Request.
        """


class PostRequest(Setting):
    name = "post_request"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called after a worker processes the request.

        The callable needs to accept two instance variables for the Worker and
        the Request.
        """
