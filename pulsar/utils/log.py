'''
Module containing utilities and mixins for logging and serialisation.
'''
import sys
import logging
from logging.config import dictConfig
from copy import deepcopy, copy
from threading import Lock
from functools import wraps

from .system import current_process
from .string import to_string
from .structures import AttributeDictionary

win32 = sys.platform == "win32"


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': ('%(asctime)s [p=%(process)s, t=%(thread)s,'
                       ' %(levelname)s, %(name)s] %(message)s'),
            'datefmt': '%H:%M:%S'
        },
        'very_verbose': {
            'format': '%(asctime)s [p=%(process)s,t=%(thread)s]'
                      ' [%(levelname)s] [%(name)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(asctime)s %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'level_message': {'format': '%(levelname)s - %(message)s'},
        'name_level_message': {
            'format': '%(name)s.%(levelname)s - %(message)s'
        },
        'message': {'format': '%(message)s'}
    },
    'handlers': {
        'silent': {
            'class': 'pulsar.utils.log.Silence',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'pulsar.utils.log.ColoredStream',
            'formatter': 'verbose'
        },
        'console_message': {
            'class': 'pulsar.utils.log.ColoredStream',
            'formatter': 'message'
        },
        'console_level_message': {
            'class': 'pulsar.utils.log.ColoredStream',
            'formatter': 'level_message'
        },
        'console_name_level_message': {
            'class': 'pulsar.utils.log.ColoredStream',
            'formatter': 'name_level_message'
        }
    },
    'filters ': {},
    'loggers': {
        'asyncio': {
            'level': 'WARNING'
        }
    },
    'root': {}
}


def file_handler(**kw):
    return logging.FileHandler('pulsar.log', **kw)


LOGGING_CONFIG['handlers']['file'] = {
    '()': file_handler,
    'formatter': 'verbose'}


def update_config(config, c):
    for name in ('handlers', 'formatters', 'filters', 'loggers', 'root'):
        if name in c:
            config[name].update(c[name])


def local_method(f):
    '''Decorator to be used in conjunction with :class:`LocalMixin` methods.
    '''
    name = f.__name__

    def _(self, *args):
        local = self.local
        if name not in local:
            setattr(local, name, f(self, *args))
        return getattr(local, name)
    return _


def local_property(f):
    '''Decorator to be used in conjunction with :class:`LocalMixin` methods.
    '''
    name = f.__name__

    def _(self):
        local = self.local
        if name not in local:
            setattr(local, name, f(self))
        return getattr(local, name)

    return property(_, doc=f.__doc__)


def lazy_string(f):
    def _(*args, **kwargs):
        return LazyString(f, *args, **kwargs)
    return _


class LazyString:
    __slots__ = ('value', 'f', 'args', 'kwargs')

    def __init__(self, f, *args, **kwargs):
        self.value = None
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        if self.value is None:
            self.value = to_string(self.f(*self.args, **self.kwargs))
        return self.value
    __repr__ = __str__


class WritelnDecorator:
    """Used to decorate file-like objects with a handy 'writeln' method.
    taken from python.
    """
    def __init__(self, stream):
        self.stream = stream

    def __getattr__(self, attr):
        if attr in ('stream', '__getstate__'):
            raise AttributeError(attr)
        return getattr(self.stream, attr)

    def writeln(self, arg=None):
        if arg:
            self.write(arg)
        self.write('\n')  # text-mode streams translate to \r\n if needed


class LocalMixin:
    '''Defines the :attr:`local` attribute.

    Classes derived from a :class:`LocalMixin` can use the
    :func:`local_method` and :func:`local_property` decorators for managing
    attributes which are not picklable.
    '''
    @property
    def local(self):
        '''A lazy :class:`pulsar.utils.structures.AttributeDictionary`.

        This attribute is removed when pickling an instance.
        '''
        if not hasattr(self, '_local'):
            self._local = AttributeDictionary()
        return self._local

    @local_property
    def lock(self):
        '''A local threading.Lock.'''
        return Lock()

    @local_property
    def process_lock(self):
        return process_global('lock')

    def clear_local(self):
        self.__dict__.pop('_local', None)

    def __getstate__(self):
        '''Remove the local dictionary.'''
        d = self.__dict__.copy()
        d.pop('_local', None)
        return d


def lazymethod(f):
    name = '_lazy_%s' % f.__name__

    @wraps(f)
    def _(self):
        if not hasattr(self, name):
            setattr(self, name, f(self))
        return getattr(self, name)

    return _


def lazyproperty(f):
    return property(lazymethod(f), doc=f.__doc__)


def process_global(name, val=None, setval=False):
    '''Access and set global variables for the current process.'''
    p = current_process()
    if not hasattr(p, '_pulsar_globals'):
        p._pulsar_globals = {'lock': Lock()}
    if setval:
        p._pulsar_globals[name] = val
    else:
        return p._pulsar_globals.get(name)


class Silence(logging.Handler):
    def emit(self, record):
        pass


def clear_logger():
    process_global('_config_logging', None, True)


def configured_logger(name=None, config=None, level=None, handlers=None):
    '''Configured logger.
    '''
    name = name or ''
    with process_global('lock'):
        logconfig = process_global('_config_logging')
        # if the logger was not configured, do so.
        if not logconfig:
            logconfig = deepcopy(config or LOGGING_CONFIG)
            logconfig['configured'] = set()
            process_global('_config_logging', logconfig, True)
        else:
            configured = logconfig.get('configured')
            if name in configured:
                return logging.getLogger(name)

        level = get_level(level)
        # No loggers configured. This means no logconfig setting
        # parameter was used. Set up the root logger with default
        # loggers
        if level == logging.NOTSET:
            handlers = ['silent']

        level = logging.getLevelName(level)
        cfg = {'level': level, 'propagate': False}
        if handlers:
            cfg['handlers'] = handlers

        config = copy(logconfig)
        configured = config.pop('configured')
        configured.add(name)

        if name:
            config.pop('root', None)
            loggers = config.pop('logger', {})
            if name in loggers:
                loggers[name].update(cfg)
                cfg = loggers[name]
            config['loggers'] = {name: cfg}
        else:
            if 'root' in config:
                config['root'].update(cfg)
                cfg = config['root']
            if 'handlers' not in cfg:
                cfg['handlers'] = ['console']
            config['root'] = cfg
        #
        dictConfig(config)
        return logging.getLogger(name)


def get_level(level):
    try:
        return int(level)
    except TypeError:
        return logging.NOTSET
    except ValueError:
        lv = str(level).upper()
        try:
            return logging._checkLevel(lv)
        except ValueError:
            return logging.NOTSET


def logger_fds():
    logger = logging.getLogger()
    loggers = set(logger.manager.loggerDict.values())
    loggers.add(logger)
    fds = set()
    for logger in loggers:
        for hnd in getattr(logger, 'handlers', ()):
            try:
                fds.add(hnd.stream.fileno())
            except AttributeError:
                pass
    return fds

WHITE = 37
COLOURS = {'red': 31,
           'green': 32,
           'yellow': 33,
           'blue': 34,
           'magenta': 35,
           'cyan': 36,
           'white': WHITE}


class ColoredStream(logging.StreamHandler):   # pragma    nocover
    bold = True
    terminator = '\n'
    COLORS = {"DEBUG": "cyan",
              "WARNING": "magenta",
              "ERROR": "red",
              "CRITICAL": "red",
              "INFO": "green"}

    def __init__(self, stream=None):
        if not stream:
            stream = sys.stdout
        logging.StreamHandler.__init__(self, stream)

    def emit(self, record):
        try:
            self.color(record)
            self.flush()
        except (KeyboardInterrupt, SystemExit):  # pragma: no cover
            raise
        except:
            self.handleError(record)

    def color(self, record):
        text = self.format(record)
        file = self.stream
        if (hasattr(file, 'isatty') and file.isatty() and
                getattr(record, 'color', True)):
            colour = self.COLORS.get(record.levelname)
            code = COLOURS.get(colour, WHITE)
            if win32:
                handle = GetStdHandle(-11)
                oldcolors = GetConsoleInfo(handle).wAttributes
                code |= (oldcolors & 0x00F0)
                if self.bold:
                    code |= FOREGROUND_INTENSITY
                SetConsoleTextAttribute(handle, code)
                while len(text) > 32768:
                    file.write(text[:32768])
                    text = text[32768:]
                if text:
                    file.write(text)
                file.write(self.terminator)
                self.flush()
                SetConsoleTextAttribute(handle, oldcolors)
            else:
                text = '\x1b[%sm%s\x1b[0m' % (code, text)
                file.write(text)
                file.write(self.terminator)
                self.flush()
        else:
            file.write(text)
            file.write(self.terminator)
            self.flush()


if win32:   # pragma    nocover
    import ctypes
    from ctypes import wintypes

    SHORT = ctypes.c_short

    class COORD(ctypes.Structure):
        _fields_ = [('X', SHORT),
                    ('Y', SHORT)]

    class SMALL_RECT(ctypes.Structure):
        _fields_ = [('Left', SHORT),
                    ('Top', SHORT),
                    ('Right', SHORT),
                    ('Bottom', SHORT)]

    class CONSOLE_SCREEN_BUFFER_INFO(ctypes.Structure):
        _fields_ = [('dwSize', COORD),
                    ('dwCursorPosition', COORD),
                    ('wAttributes', wintypes.WORD),
                    ('srWindow', SMALL_RECT),
                    ('dwMaximumWindowSize', COORD)]

    WHITE = 0x0007
    FOREGROUND_INTENSITY = 0x0008
    COLOURS = {'red': 0x0004,
               'green': 0x0002,
               'yellow': 0x0006,
               'blue': 0x0001,
               'magenta': 0x0005,
               'cyan': 0x0003,
               'white': WHITE}

    _GetStdHandle = ctypes.windll.kernel32.GetStdHandle
    _GetStdHandle.argtypes = [wintypes.DWORD]
    _GetStdHandle.restype = wintypes.HANDLE

    def GetStdHandle(kind):
        return _GetStdHandle(kind)

    SetConsoleTextAttribute = ctypes.windll.kernel32.SetConsoleTextAttribute
    SetConsoleTextAttribute.argtypes = [wintypes.HANDLE, wintypes.WORD]
    SetConsoleTextAttribute.restype = wintypes.BOOL

    _GetConsoleScreenBufferInfo = \
        ctypes.windll.kernel32.GetConsoleScreenBufferInfo
    _GetConsoleScreenBufferInfo.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(CONSOLE_SCREEN_BUFFER_INFO)]
    _GetConsoleScreenBufferInfo.restype = wintypes.BOOL

    def GetConsoleInfo(handle):
        info = CONSOLE_SCREEN_BUFFER_INFO()
        _GetConsoleScreenBufferInfo(handle, info)
        return info
