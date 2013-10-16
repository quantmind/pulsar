'''
Module containing utilities and mixins for logging and serialisation.
'''
import sys
from copy import deepcopy, copy
from time import time
import logging
from threading import Lock
from multiprocessing import current_process

win32 = sys.platform == "win32"

if sys.version_info < (2, 7):    # pragma    nocover
    from .fallbacks._dictconfig import dictConfig

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    from logging.config import dictConfig
    from logging import NullHandler

from .structures import AttributeDictionary

NOLOG = 100

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    'none': None
}


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s [p=%(process)s,t=%(thread)s]'
                      ' [%(levelname)s] [%(name)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(asctime)s %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
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
        }
    },
    'filters ': {},
    'loggers': {},
    'root': {}
}


def update_config(config, c):
    for name in ('handlers', 'formatters', 'filters', 'loggers', 'root'):
        if name in c:
            config[name].update(c[name])


def local_method(f):
    '''Decorator to be used in conjunction with :class:`LocalMixin` methods.
    '''
    name = f.__name__

    def _(self):
        local = self.local
        if name not in local:
            setattr(local, name, f(self))
        return getattr(local, name)
    return _


def local_property(f):
    '''Decorator to be used in conjunction with :class:`LocalMixin` methods.
    '''
    return property(local_method(f), doc=f.__doc__)


class WritelnDecorator(object):
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


class LocalMixin(object):
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

    def _(self):
        if not hasattr(self, name):
            setattr(self, name, f(self))
        return getattr(self, name)
    _.__doc__ = f.__doc__
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


class LogginMixin(LocalMixin):
    '''A :class:`LocalMixin` used throughout the library.

    It provides built in logging object and utilities for pickle.
    '''
    @property
    def logger(self):
        '''A python logger for this instance.'''
        return self.local.logger

    def __setstate__(self, state):
        self.__dict__ = state
        info = getattr(self, '_log_info', {})
        self.configure_logging(**info)

    def configure_logging(self, logger=None, config=None, level=None,
                          handlers=None):
        '''Configure logging.

        This function is invoked every time an instance of this class is
        un-serialised (possibly in a different process domain).
        '''
        with self.process_lock:
            logconfig = original = process_global('_config_logging')
            # if the logger was not configured, do so.
            if not logconfig:
                logconfig = deepcopy(LOGGING_CONFIG)
                if config:
                    update_config(logconfig, config)
                original = logconfig
                process_global('_config_logging', logconfig, True)
            else:
                logconfig = deepcopy(logconfig)
                logconfig['disable_existing_loggers'] = False
                logconfig.pop('loggers', None)
                logconfig.pop('root', None)
            if level is None:
                level = logging.NOTSET
            else:
                try:
                    level = int(level)
                except (ValueError):
                    lv = str(level).upper()
                    if lv in logging._levelNames:
                        level = logging._levelNames[lv]
                    else:
                        level = logging.NOTSET
            # No loggers configured. This means no logconfig setting
            # parameter was used. Set up the root logger with default
            # loggers
            if level == logging.NOTSET:
                handlers = ['silent']
            else:
                handlers = handlers or ['console']
            level = logging.getLevelName(level)
            logger = logger or self.__class__.__name__.lower()
            if logger not in original['loggers']:
                if 'loggers' not in logconfig:
                    logconfig['loggers'] = {}
                l = {'level': level, 'handlers': handlers, 'propagate': False}
                original['loggers'][logger] = l
                logconfig['loggers'][logger] = l
            if not original.get('root'):
                logconfig['root'] = {'handlers': handlers,
                                     'level': level}
            if logconfig:
                dictConfig(logconfig)
            self._log_info = {'logger': logger,
                              'level': level,
                              'handlers': handlers,
                              'config': config}
            self.local.logger = logging.getLogger(logger)


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
        if file.isatty() or True:
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
