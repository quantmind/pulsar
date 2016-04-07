"""Configuration utilities which provides pulsar with configuration parameters
which can be parsed from the command line. Parsing is implemented using
the python argparser_ standard library module.

Config
~~~~~~~~~~

.. autoclass:: Config
   :members:
   :member-order: bysource

Setting
~~~~~~~~~~

.. autoclass:: Setting
   :members:
   :member-order: bysource


.. _argparser: http://docs.python.org/dev/library/argparse.html
"""
import inspect
import argparse
import os
import textwrap
import logging
import pickle
import types

from pulsar import __version__, SERVER_NAME
from . import system
from .string import camel_to_dash
from .internet import parse_address
from .importer import import_system_file
from .log import configured_logger
from .pep import to_bytes


__all__ = ['Config',
           'Setting',
           'ordered_settings',
           'validate_string',
           'validate_callable',
           'validate_bool',
           'validate_list',
           'validate_dict',
           'validate_pos_int',
           'validate_pos_float',
           'make_optparse_options']

LOGGER = logging.getLogger('pulsar.config')

section_docs = {}
KNOWN_SETTINGS = {}
KNOWN_SETTINGS_ORDER = []


def pass_through(arg):
    """A dummy function accepting one parameter only.

    It does nothing and it is used as default by
    :ref:`Application Hooks <setting-section-application-hooks>`.
    """
    pass


def set_if_avail(container, key, value, *skip_values):
    if value is not None and value not in skip_values:
        container[key] = value


def wrap_method(func):
    def _wrapped(instance, *args, **kwargs):
        return func(*args, **kwargs)
    return _wrapped


def ordered_settings():
    for name in KNOWN_SETTINGS_ORDER:
        yield KNOWN_SETTINGS[name]


simple_values = (list, tuple, float, int, dict, str, types.FunctionType)


def valid_config_value(val):
    if isinstance(val, simple_values):
        try:
            pickle.loads(pickle.dumps(val))
            return True
        except Exception:
            pass
    return False


class Config:
    """A dictionary-like container of :class:`Setting` parameters for
    fine tuning pulsar servers.

    It provides easy access to :attr:`Setting.value`
    attribute by exposing the :attr:`Setting.name` as attribute.

    :param description: description used when parsing the command line,
        same usage as in the :class:`argparse.ArgumentParser` class.
    :param epilog: epilog used when parsing the command line, same usage
        as in the :class:`argparse.ArgumentParser` class.
    :param version: version used when parsing the command line, same usage
        as in the :class:`argparse.ArgumentParser` class.
    :param apps: list of application namespaces to include in the
        :attr:`settings` dictionary. For example if ``apps`` is set to
        ``['socket', 'test']``, the
        :ref:`socket server <setting-section-socket-servers>` and
        :ref:`task queue <setting-section-test>` settings are
        loaded in addition to the standard
        :ref:`global settings <setting-section-global-server-settings>`,
        :ref:`worker settings <setting-section-worker-processes>` and
        :ref:`hook settings <setting-section-application-hooks>`.

    .. attribute:: settings

        Dictionary of all :class:`Setting` instances available in this
        :class:`Config` container.

        Keys are given by the :attr:`Setting.name` attribute.

    .. attribute:: params

        Dictionary of additional parameters which cannot be parsed on the
        command line
    """
    script = None
    application = None
    exclude_from_config = set(('config',))

    def __init__(self, description=None, epilog=None,
                 version=None, apps=None, include=None,
                 exclude=None, settings=None, prefix=None,
                 name=None, log_name=None, **params):
        self.settings = {} if settings is None else settings
        self.params = {}
        self.name = name
        self.log_name = log_name
        self.prefix = prefix
        self.include = set(include or ())
        self.exclude = set(exclude or ())
        self.apps = set(apps or ())
        if settings is None:
            self.update_settings()
        self.description = description or 'Pulsar server'
        self.epilog = epilog or 'Have fun!'
        self.version = version or __version__
        self.update(params, True)

    def __iter__(self):
        return iter(self.settings)

    def __len__(self):
        return len(self.settings)

    def __contains__(self, name):
        return name in self.settings

    def items(self):
        for k, setting in self.settings.items():
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
        except KeyError as exc:
            raise AttributeError("'%s' object has no attribute '%s'." %
                                 (self.__class__.__name__, name)) from exc

    def __setattr__(self, name, value):
        if name != "settings" and name in self.settings:
            raise AttributeError("Invalid access!")
        super().__setattr__(name, value)

    def update(self, data, default=False):
        """Update this :attr:`Config` with ``data``.

        :param data: must be a ``Mapping`` like object exposing the ``item``
            method for iterating through key-value pairs.
        :param default: if ``True`` the updated :attr:`settings` will also
            set their :attr:`~Setting.default` attribute with the
            updating value (provided it is a valid one).
        """
        for name, value in data.items():
            if value is not None:
                self.set(name, value, default)

    def copy_globals(self, cfg):
        """Copy global settings from ``cfg`` to this config.

        The settings are copied only if they were not already modified.
        """
        for name, setting in cfg.settings.items():
            csetting = self.settings.get(name)
            if (setting.is_global and csetting is not None and
                    not csetting.modified):
                csetting.set(setting.get())

    def get(self, name, default=None):
        """Get the value at ``name`` for this :class:`Config` container

        The returned value is obtained from:

        * the value at ``name`` in the :attr:`settings` dictionary
          if available.
        * the value at ``name`` in the :attr:`params` dictionary if available.
        * the ``default`` value.
        """
        try:
            return self._get(name, default)
        except KeyError:
            return default

    def set(self, name, value, default=False, imported=False):
        """Set the :class:`Setting` at ``name`` with a new ``value``.

        If ``default`` is ``True``, the :attr:`Setting.default` is also set.
        """
        if name in self.__dict__:
            self.__dict__[name] = value
            return

        if name not in self.settings and self.prefix:
            prefix_name = '%s_%s' % (self.prefix, name)
            if prefix_name in self.settings:
                return  # don't do anything

        if name in self.settings:
            self.settings[name].set(value, default=default, imported=imported)
        elif not imported:
            self.params[name] = value

    def parser(self):
        """Create the argparser_ for this configuration by adding all
        settings via the :meth:`Setting.add_argument` method.

        :rtype: an instance of :class:`ArgumentParser`.
        """
        parser = argparse.ArgumentParser(description=self.description,
                                         epilog=self.epilog)
        parser.add_argument('--version',
                            action='version',
                            version=self.version)
        return self.add_to_parser(parser)

    def add_to_parser(self, parser):
        """Add this container :attr:`settings` to an existing ``parser``.
        """
        setts = self.settings

        def sorter(x):
            return (setts[x].section, setts[x].order)

        for k in sorted(setts, key=sorter):
            setts[k].add_argument(parser)
        return parser

    def import_from_module(self, mod=None):
        if mod:
            self.set('config', mod)
        try:
            mod = import_system_file(self.config)
        except Exception as e:
            raise RuntimeError('Failed to read config file "%s". %s' %
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
                    if valid_config_value(val):
                        unknowns.append((k, val))
                else:
                    self.set(kl, val, True, True)
        return unknowns

    def parse_command_line(self, argv=None):
        """Parse the command line
        """
        if self.config:
            parser = argparse.ArgumentParser(add_help=False)
            self.settings['config'].add_argument(parser)
            opts, _ = parser.parse_known_args(argv)
            if opts.config is not None:
                self.set('config', opts.config)

            self.params.update(self.import_from_module())

        parser = self.parser()
        opts = parser.parse_args(argv)
        for k, v in opts.__dict__.items():
            if v is None:
                continue
            self.set(k.lower(), v)

    def on_start(self):
        """Invoked by a :class:`.Application` just before starting.
        """
        for sett in self.settings.values():
            sett.on_start()

    def app(self):
        if self.application:
            return self.application.from_config(self)

    @property
    def workers(self):
        """Number of workers
        """
        workers = self.settings.get('workers')
        return workers.get() if workers else 0

    @property
    def address(self):
        """An address to bind to, only available if a
        :ref:`bind <setting-bind>` setting has been added to this
        :class:`Config` container.
        """
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
        """A copy of this :class:`Config` container.

        If ``prefix`` is given, it prefixes all non
        :ref:`global settings <setting-section-global-server-settings>`
        with it. Used when multiple applications are loaded.
        """
        cls = self.__class__
        me = cls.__new__(cls)
        me.__dict__.update(self.__dict__)
        if prefix:
            me.prefix = prefix
        settings = me.settings
        me.settings = {}
        for setting in settings.values():
            setting = setting.copy(name, prefix)
            me.settings[setting.name] = setting
        me.params = me.params.copy()
        return me

    def clone(self):
        return pickle.loads(pickle.dumps(self))

    def configured_logger(self, name=None):
        """Configured logger.
        """
        log_handlers = self.log_handlers
        # logname
        if not name:
            # base name is always pulsar
            basename = 'pulsar'
            # the namespace name for this config
            name = self.name
            if name and name != basename:
                name = '%s.%s' % (basename, name)
            else:
                name = basename
        #
        namespaces = {}

        for log_level in self.log_level or ():
            bits = log_level.split('.')
            namespaces['.'.join(bits[:-1]) or ''] = bits[-1]

        for namespace in sorted(namespaces):
            if self.daemon:     # pragma    nocover
                handlers = []
                for hnd in log_handlers:
                    if hnd != 'console':
                        handlers.append(hnd)
                if not handlers:
                    handlers.append('file')
                log_handlers = handlers
            configured_logger(namespace,
                              config=self.log_config,
                              level=namespaces[namespace],
                              handlers=log_handlers)
        return logging.getLogger(name)

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo):
        return self.copy()

    ########################################################################
    #    INTERNALS
    def update_settings(self):
        for s in ordered_settings():
            setting = s().copy(name=self.name, prefix=self.prefix)
            if setting.name in self.settings:
                continue
            if setting.app and setting.app not in self.apps:
                continue    # the setting is for an app not in the apps set
            if ((self.include and setting.name not in self.include) or
                    setting.name in self.exclude):
                self.params[setting.name] = setting.get()
            else:
                self.settings[setting.name] = setting

    def _get(self, name, default=None):
        if name not in self.settings:
            if name in self.params:
                return self.params[name]
            if name in KNOWN_SETTINGS:
                return default
            raise KeyError("'%s'" % name)
        return self.settings[name].get()


class SettingMeta(type):
    """A metaclass which collects all setting classes and put them
    in the global ``KNOWN_SETTINGS`` list.
    """
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        # parents = [b for b in bases if isinstance(b, SettingMeta)]
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
            new_class.name = camel_to_dash(new_class.__name__)
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


class Setting(metaclass=SettingMeta):
    """Class for creating :ref:`pulsar settings <settings>`.

    Most parameters can be specified on the command line,
    all of them on a ``config`` file.
    """
    creation_count = 0
    virtual = True
    """If set to ``True`` the settings won't be loaded.

    It can be only used as base class for other settings."""
    name = None
    """The key to access this setting in a :class:`Config` container."""
    validator = None
    """A validating function for this setting.

    It provided it must be a function accepting one positional argument,
    the value to validate."""
    value = None
    imported = False
    """The actual value for this setting."""
    default = None
    """The default value for this setting."""
    nargs = None
    """The number of command-line arguments that should be consumed"""
    const = None
    """A constant value required by some action and nargs selections"""
    app = None
    """Setting for a specific :class:`Application`."""
    section = None
    """Setting section, used for creating the
    :ref:`settings documentation <settings>`."""
    flags = None
    """List of options strings, e.g. ``[-f, --foo]``."""
    choices = None
    """Restrict the argument to the choices provided."""
    type = None
    """The type to which the command-line argument should be converted"""
    meta = None
    """Same usage as ``metavar`` in the python :mod:`argparse` module. It is
    the name for the argument in usage message."""
    action = None
    """The basic type of action to be taken when this argument is encountered
    at the command line"""
    short = None
    """Optional shot description string"""
    desc = None
    """Description string"""
    is_global = False
    """``True`` only for
    :ref:`global settings <setting-section-global-server-settings>`."""
    orig_name = None

    def __init__(self, name=None, flags=None, action=None, type=None,
                 default=None, nargs=None, desc=None, validator=None,
                 app=None, meta=None, choices=None, const=None):
        self.extra = e = {}
        self.app = app or self.app
        set_if_avail(e, 'choices', choices or self.choices)
        set_if_avail(e, 'const', const or self.const)
        set_if_avail(e, 'type', type or self.type, 'string')
        self.default = default if default is not None else self.default
        self.desc = desc or self.desc
        self.flags = flags or self.flags
        self.action = action or self.action
        self.meta = meta or self.meta
        self.name = name or self.name
        self.nargs = nargs or self.nargs
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
        self.modified = False

    def __getstate__(self):
        data = self.__dict__.copy()
        if self.imported:
            data.pop('imported', None)
            data.pop('value', None)
            data.pop('default', None)
        return data

    def __str__(self):
        return '{0} ({1})'.format(self.name, self.value)
    __repr__ = __str__

    def on_start(self):
        """Called when pulsar server starts.

        It can be used to perform custom initialization for this
        :class:`Setting`.
        """
        pass

    def get(self):
        """Returns :attr:`value`"""
        return self.value

    def set(self, val, default=False, imported=False):
        """Set ``val`` as the :attr:`value` for this :class:`Setting`.

        If ``default`` is ``True`` set also the :attr:`default` value.
        """
        if hasattr(self.validator, '__call__'):
            val = self.validator(val)
        self.value = val
        self.imported = imported
        if default:
            self.default = val
        self.modified = True

    def add_argument(self, parser, set_default=False):
        """Add this :class:`Setting` to the ``parser``.

        The operation is carried out only if :attr:`flags` or
        :attr:`nargs` and :attr:`name` are defined.
        """
        default = self.default if set_default else None
        kwargs = {'nargs': self.nargs}
        kwargs.update(self.extra)
        if self.flags:
            args = tuple(self.flags)
            kwargs.update({'dest': self.name,
                           'action': self.action or "store",
                           'default': default,
                           'help': "%s [%s]" % (self.short, self.default)})
            if kwargs["action"] != "store":
                kwargs.pop("type", None)
                kwargs.pop("nargs", None)
        elif self.nargs and self.name:
            args = (self.name,)
            kwargs.update({'metavar': self.meta or None,
                           'help': self.short})
        else:
            # Not added to argparser
            return
        if self.meta:
            kwargs['metavar'] = self.meta
        parser.add_argument(*args, **kwargs)

    def copy(self, name=None, prefix=None):
        """Copy this :class:`SettingBase`"""
        setting = self.__class__.__new__(self.__class__)
        setting.__dict__.update(self.__dict__)
        # Keep the modified flag?
        # setting.modified = False
        if prefix and not setting.is_global:
            flags = setting.flags
            # Prefix a setting
            setting.orig_name = setting.name
            setting.name = '%s_%s' % (prefix, setting.name)
            if flags and flags[-1].startswith('--'):
                setting.flags = ['--%s-%s' % (prefix, flags[-1][2:])]
        if name and not setting.is_global:
            setting.short = '%s application. %s' % (name, setting.short)
        return setting

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo):
        return self.copy()


class TestOption(Setting):
    virtual = True
    app = 'test'
    section = "Test"


def validate_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return bool(val)
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
    if val is None:
        return None
    if not isinstance(val, str):
        raise TypeError("Not a string: %s" % val)
    return val.strip()


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
        if not hasattr(val, '__call__'):
            raise TypeError("Value is not callable: %s" % val)
        if not inspect.isfunction(val):
            cval = val.__call__
            discount = 1
        else:
            discount = 0
            cval = val
        result = inspect.getargspec(cval)
        nargs = len(result.args) - discount
        if result.defaults:
            group = tuple(range(nargs-len(result.defaults), nargs+1))
        else:
            group = (nargs,)
        if arity not in group:
            raise TypeError("Value must have an arity of: %s" % arity)
        return val
    return _validate_callable


def make_optparse_options(apps=None, exclude=None, include=None, **override):
    """Create a tuple of optparse options."""
    from optparse import make_option

    class AddOptParser(list):
        def add_argument(self, *args, **kwargs):
            if 'const' in kwargs:
                kwargs['action'] = 'store_const'
                kwargs.pop('type')
            self.append(make_option(*args, **kwargs))

    config = Config(apps=apps, exclude=exclude, include=include, **override)
    parser = AddOptParser()
    config.add_to_parser(parser)
    return tuple(parser)


############################################################################
#    Global Server Settings
section_docs['Global Server Settings'] = """
These settings are global in the sense that they are used by the arbiter
as well as all pulsar workers. They are server configuration parameters.
"""


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
        if self.value:  # pragma    nocover
            os.environ['http_proxy'] = self.value
            os.environ['https_proxy'] = self.value
            os.environ['ws_proxy'] = self.value
            os.environ['wss_proxy'] = self.value


class Debug(Global):
    flags = ["--debug"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Turn on debugging.

        Set the log level to debug, limits the number of worker processes
        to 1, set asyncio debug flag.
        """


class Daemon(Global):
    flags = ["-D", "--daemon"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Daemonize the pulsar process (posix only).

        Detaches the server from the controlling terminal and enters the
        background.
        """


class Reload(Global):
    flags = ["--reload"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """\
        Auto reload modules when changes occurs.

        Useful during development.
        """


class PidFile(Global):
    flags = ["-p", "--pid-file"]
    meta = "FILE"
    validator = validate_string
    desc = """\
        A filename to use for the PID file.

        If not set, no PID file will be written.
        """


class Password(Global):
    flags = ["--password"]
    validator = validate_string
    desc = """Set a password for the server"""


class User(Global):
    flags = ["-u", "--user"]
    meta = "USER"
    validator = validate_string
    desc = """\
        Switch worker processes to run as this user.

        A valid user id (as an integer) or the name of a user that can be
        retrieved with a call to pwd.getpwnam(value) or None to not change
        the worker process user.
        """


class Group(Global):
    flags = ["-g", "--group"]
    meta = "GROUP"
    validator = validate_string
    desc = """\
        Switch worker process to run as this group.

        A valid group id (as an integer) or the name of a user that can be
        retrieved with a call to pwd.getgrnam(value) or None to not change
        the worker processes group.
        """


class LogLevel(Global):
    flags = ["--log-level"]
    nargs = '+'
    default = ['info']
    validator = validate_list
    desc = """
        The granularity of log outputs.

        This setting controls loggers with ``pulsar`` namespace
        and the the root logger (if not already set).
        Valid level names are:

        * debug
        * info
        * warning
        * error
        * critical
        * none
        """


class LogHandlers(Global):
    flags = ["--log-handlers"]
    nargs = '+'
    default = ['console']
    validator = validate_list
    desc = """Log handlers for pulsar server"""


class LogConfig(Global):
    default = {}
    validator = validate_dict
    desc = """
    The logging configuration dictionary.

    This settings can only be specified on a config file and therefore
    no command-line parameter is available.
    """


class ProcessName(Global):
    flags = ["-n", "--process-name"]
    meta = "STRING"
    validator = validate_string
    default = None
    desc = """\
        A base to use with setproctitle for process naming.

        This affects things like ``ps`` and ``top``. If you're going to be
        running more than one instance of Pulsar you'll probably want to set a
        name to tell them apart. This requires that you install the
        setproctitle module.

        It defaults to 'pulsar'.
        """


class DefaultProcessName(Global):
    validator = validate_string
    default = SERVER_NAME
    desc = """\
        Internal setting that is adjusted for each type of application.
        """


class Coverage(Global):
    flags = ["--coverage"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = """Collect code coverage from all spawn actors."""


class DataStore(Global):
    flags = ['--data-store']
    meta = "CONNECTION STRING"
    default = ''
    desc = """\
    Default data store.

    Use this setting to specify a datastore used by pulsar applications.
    By default no datastore is used.
    """


class ExecutionId(Global):
    name = 'exc_id'
    flags = ['--exc-id']
    default = ''
    desc = """\
    Execution ID.

    Use this setting to specify an execution ID.
    If not provided, a value will be assigned by pulsar.
    """


class UseGreenlet(Global):
    name = 'greenlet'
    flags = ['--greenlet']
    nargs = '?'
    type = int
    default = 0
    const = 100
    desc = """\
    Use greenlet whenever possible.
    """

############################################################################
#    Worker Processes
section_docs['Worker Processes'] = """
This group of configuration parameters control the number of actors
for a given :class:`.Monitor`, the type of concurreny of the server and
other actor-specific parameters.

They are available to all applications and, unlike global settings,
each application can specify different values.
"""


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
        the ``2-4 x NUM_CORES`` range should be good. If you are using
        threads this number can be higher.
        """


class Concurrency(Setting):
    name = "concurrency"
    section = "Worker Processes"
    choices = ('process', 'thread')
    flags = ["--concurrency"]
    default = "process"
    desc = """The type of concurrency to use."""


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
    default = 5
    desc = """\
        Maximum number of threads used by the actor event loop executor.

        The executor is a thread pool used by the event loop to perform CPU
        intensive operations or when it needs to execute blocking calls.
        It allows the actor main thread to be free to listen
        to events on file descriptors and process them as quick as possible.
        """


############################################################################
#    APPLICATION HOOKS
section_docs['Application Hooks'] = """
Application hooks are functions which can be specified in a
:ref:`config <setting-config>` file to perform custom tasks in a pulsar server.
These tasks can be scheduled when events occurs or at every event loop of
the various components of a pulsar application.

All application hooks are functions which accept one positional
parameter and one key-valued parameter ``exc`` when an exception occurs::

    def hook(arg, exc=None):
        ...

Like worker process settings, each application can specify their own.
"""


class Postfork(Setting):
    name = "post_fork"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = 'Called just after a worker has been forked'


class WhenReady(Setting):
    name = "when_ready"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = 'Called just before a worker starts running its event loop'


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

        The callable needs to accept one parameter for the
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

        The callable needs to accept one parameter for the
        connection instance.
        """


class PreRequest(Setting):
    name = "pre_request"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called just before an application server processes a request.

        The callable needs to accept one parameters for the
        consumer.
        """


class PostRequest(Setting):
    name = "post_request"
    section = "Application Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(pass_through)
    desc = """\
        Called after an application server processes a request.

        The callable needs to accept one parameter for the
        consumer.
        """
