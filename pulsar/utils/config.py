'''\
Configuration utilities. originally from gunicorn_,
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

from pulsar import __version__, SERVER_NAME
from pulsar.utils import system
from pulsar.utils.py2py3 import *


__all__ = ['Config',
           'Setting',
           'ordered_settings',
           'validate_string',
           'validate_callable',
           'validate_bool',
           'validate_list',
           'validate_pos_int',
           'make_settings',
           'make_options']

    
KNOWN_SETTINGS = {}
KNOWN_SETTINGS_ORDER = []


def def_start_server(server):
    pass
    

def def_pre_exec(server):
    pass
    
    
def default_process(worker):
    pass


def def_pre_request(worker, req):
    worker.log.debug("%s %s" % (req.method, req.path))


def def_post_request(worker, req):
    pass


def def_worker_exit(worker):
    pass
    
    
def wrap_method(func):
    def _wrapped(instance, *args, **kwargs):
        return func(*args, **kwargs)
    return _wrapped

    
def ordered_settings():
    for name in KNOWN_SETTINGS_ORDER:
        yield KNOWN_SETTINGS[name]
        
        
def make_settings(app = None, include=None, exclude=None):
    '''Creates a dictionary of available settings for a given
application *app*.

:parameter app: Optional application name.
:parameter include: Optional list of settings to include.
:parameter app: Optional list of settings to exclude.
:rtype: dictionary of :class:`pulsar.Setting` instances.'''
    settings = {}
    exclude = exclude or ()
    for s in ordered_settings():
        setting = s()
        if setting.app and setting.app != app:
            continue
        if include and setting.name not in include and not setting.app:
            continue
        if setting.name in exclude:
            continue
        settings[setting.name] = setting.copy()
    return settings


def make_options():
    g_settings = make_settings(exclude=('version',))

    keys = g_settings.keys()
    def sorter(k):
        return (g_settings[k].section, g_settings[k].order)

    opts = []
    
    for k in keys:
        setting = g_settings[k]
        if not setting.flags:
            continue

        args = tuple(setting.flags)

        kwargs = {
            "dest": setting.name,
            "metavar": setting.meta or None,
            "action": setting.action or "store",
            "type": setting.type or "string",
            "default": None,
            "help": "%s [%s]" % (setting.short, setting.default)
        }
        if kwargs["action"] != "store":
            kwargs.pop("type")

        opts.append(optparse.make_option(*args, **kwargs))

    return tuple(opts)


class Config(object):
    '''Dictionary containing :class:`Setting` parameters for
fine tuning pulsar servers. It provides easy access to :attr:`Setting.value`
attribute by exposing the :attr:`Setting.name` as attribute.

.. attribute:: settings

    Dictionary of all :class:`Settings` instances available. The
    keys are given by the :attr:`Setting.name` attribute.
'''
    def __init__(self, description = None, epilog = None,
                 version = None, app = None, include=None,
                 exclude = None):
        self.settings = make_settings(app,include,exclude)
        self.description = description or 'Pulsar server'
        self.epilog = epilog or 'Have fun!'
        self.version = version or __version__
        
    def __getstate__(self):
        return self.__dict__.copy()
    
    def __setstate__(self, state):
        for k,v in state.items():
            self.__dict__[k] = v
    
    def __getattr__(self, name):
        if name not in self.settings:
            if name in KNOWN_SETTINGS:
                return None
            raise AttributeError("No configuration setting for: %s" % name)
        return self.settings[name].get()
    
    def __setattr__(self, name, value):
        if name != "settings" and name in self.settings:
            raise AttributeError("Invalid access!")
        super(Config, self).__setattr__(name, value)
    
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
                            version = self.version)
        setts = self.settings
        sorter = lambda x: (setts[x].section, setts[x].order)
        for k in sorted(setts,key=sorter):
            setts[k].add_argument(parser)
        return parser

    def on_start(self):
        for sett in self.settings.values():
            sett.on_start()
    
    @property
    def workers(self):
        return self.settings['workers'].get()

    @property
    def address(self):
        bind = self.settings['bind']
        if bind:
            return system.parse_address(to_bytestring(bind.get()))
        
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
        pn = self.settings.get('proc_name')
        if pn:
            pn = pn.get()
        if pn is not None:
            return pn
        else:
            pn = self.settings.get('default_proc_name')
            if pn:
                return pn.get()
            
            
class SettingMeta(type):
    '''A metaclass which collects all setting classes and put them
in the global ``KNOWN_SETTINGS`` list.'''
    def __new__(cls, name, bases, attrs):
        super_new = super(SettingMeta, cls).__new__
        parents = [b for b in bases if isinstance(b, SettingMeta)]
        if not parents or attrs.pop('virtual',False):
            return super_new(cls, name, bases, attrs)            
    
        attrs["order"] = len(KNOWN_SETTINGS)
        val = attrs.get("validator")
        attrs["validator"] = wrap_method(val) if val else None
        
        new_class = super_new(cls, name, bases, attrs)
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
        
        
# This works for Python 2 and Python 3
class Setting(SettingMeta('BaseSettings', (object,), {'virtual': True})):
    '''A configuration parameter for pulsar. Parameters can be specified
on the command line or on a config file.'''
    virtual = True
    '''If set to ``True`` the settings won't be loaded and it can be only used
as base class for other settings.'''
    name = None
    '''The unique name used to access this setting.'''
    nargs = None
    '''For positional arguments. Same usage as argparse.'''
    app = None
    '''Setting for a specific :class:`Application`.'''
    value = None
    '''The actual value for this setting.'''
    section = None
    '''Setting section, used for creating documentation.'''
    flags = None
    '''List of options strings, e.g. ``[-f, --foo]``.'''
    choices = None
    '''Restrict the argument to the choices provided.'''
    validator = None
    '''Validator for this setting. It provided it must be a fucntion
accepting one positional argument, the value to validate.'''
    type = None
    meta = None
    action = None
    default = None
    '''Default value'''
    short = None
    desc = None
    
    def __init__(self):
        if self.default is not None:
            self.set(self.default)
        self.short = self.short or self.desc
        self.desc = self.desc or self.short
        if self.app and not self.section:
            self.section = self.app
    
    def __getstate__(self):
        d = self.__dict__.copy()
        return d
    
    def __str__(self):
        return '{0} ({1})'.format(self.name,self.value)
    __repr__ = __str__
        
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
    
    def get(self):
        return self.value
    
    def set(self, val):
        if hasattr(self.validator,'__call__'):
            val = self.validator(val)
        self.value = val
        
    def on_start(self):
        pass


def validate_bool(val):
    if isinstance(val,bool):
        return val
    if not isinstance(val, string_type):
        raise TypeError("Invalid type for casting: %s" % val)
    if val.lower().strip() == "true":
        return True
    elif val.lower().strip() == "false":
        return False
    else:
        raise ValueError("Invalid boolean: %s" % val)


def validate_pos_int(val):
    if not isinstance(val,int_type):
        val = int(val, 0)
    else:
        # Booleans are ints!
        val = int(val)
    if val < 0:
        raise ValueError("Value must be positive: %s" % val)
    return val


def validate_string(val):
    if val is None:
        return None
    if not is_bytes_or_string(val):
        raise TypeError("Not a string: %s" % val)
    return to_string(val).strip()


def validate_list(val):
    if val and not isinstance(val,list):
        raise TypeError("Not a list: %s" % val)
    return val


def validate_dict(val):
    if val and not isinstance(val,dict):
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
    name = "workers"
    section = "Worker Processes"
    flags = ["-w", "--workers"]
    validator = validate_pos_int
    type = int
    default = 1
    desc = """\
        The number of worker process for handling requests.
        
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


class Timeout(Setting):
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
        
class HttpClient(Setting):
    name = "http_client"
    section = "Http"
    flags = ["--http-client"]
    type = int
    default = 2
    desc = """\
        Set the python parser as default HTTP parser.    
        """
    
    def on_start(self):
        if self.value is not 2:
            v = (self.value,)
            if self.value is not 1:
                v += (1,)
            from pulsar.net import setDefaultClient
            setDefaultClient(v)
        
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
            from pulsar.lib import setDefaultHttpParser, fallback
            setDefaultHttpParser(fallback.HttpParser)


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
        Daemonize the Pulsar process.
        
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


class Umask(Setting):
    name = "umask"
    section = "Server Mechanics"
    flags = ["-m", "--umask"]
    validator = validate_pos_int
    type = int
    default = 0
    desc = """\
        A bit mask for the file mode on files written by Gunicorn.
        
        Note that this affects unix socket permissions.
        
        A valid value for the os.umask(mode) call or a string compatible with
        int(value, 0) (0 means Python guesses the base, so values like "0",
        "0xFF", "0022" are valid for decimal, hex, and octal representations)
        """


class TmpUploadDir(Setting):
    name = "tmp_upload_dir"
    section = "Server Mechanics"
    meta = "DIR"
    validator = validate_string
    default = None
    desc = """\
        Directory to store temporary request data as they are read.
        
        This may disappear in the near future.
        
        This path should be writable by the process permissions set for Gunicorn
        workers. If not specified, Gunicorn will choose a system generated
        temporary directory.
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
    name = "proc_name"
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
    name = "default_proc_name"
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
    default = staticmethod(def_start_server)
    desc = """\
        Called just after the server is started.
        
        The callable needs to accept a single instance variable for the Arbiter.
        """


class Prefork(Setting):
    name = "pre_fork"
    section = "Server Hooks"
    validator = validate_callable(1)
    default = staticmethod(default_process)
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
    default = staticmethod(default_process)
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
    default = staticmethod(def_pre_exec)
    desc = """\
        Called just before a new master process is forked.
        
        The callable needs to accept a single instance variable for the Arbiter.
        """


class PreRequest(Setting):
    name = "pre_request"
    section = "Server Hooks"
    validator = validate_callable(2)
    type = "callable"
    default = staticmethod(def_pre_request)
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
    default = staticmethod(def_post_request)
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
    default = staticmethod(def_worker_exit)
    desc = """Called just after a worker has been exited.

        The callable needs to accept one variable for the
        the just-exited Worker.
        """


class WorkerTask(Setting):
    name = "worker_task"
    section = "Server Hooks"
    validator = validate_callable(1)
    type = "callable"
    default = staticmethod(def_worker_exit)
    desc = """Called at every event loop by the worker.

        The callable needs to accept one variable for the Worker.
        """