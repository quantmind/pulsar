import sys
from copy import deepcopy, copy
from time import time
import logging
from threading import Lock
from multiprocessing import current_process

if sys.version_info < (2,7):    #pragma    nocover
    from .fallbacks._dictconfig import dictConfig
    
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    from logging.config import dictConfig
    from logging import NullHandler
    
from .structures import AttributeDictionary

NOLOG = 100

__all__ = ['dictConfig',
           'NullHandler',
           'process_global',
           'LogginMixin',
           'Synchronized',
           'local_method',
           'local_property',
           'threadsafe',
           'LocalMixin']


LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    'none': None
}

def install_new_log_level(level, name):
    logging.addLevelName(level, name.upper())
    new_log = lambda self, msg, *args, **kws: self._log(level, msg, args, **kws)
    name = name.lower()
    setattr(logging.Logger, name, new_log)
    LOG_LEVELS[name] = level

install_new_log_level(5, 'subdebug')


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s [p=%(process)s,t=%(thread)s]'
                      ' [%(levelname)s] [%(name)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'verbose_color': {
            '()': 'pulsar.utils.tools.ColorFormatter',
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
            'class': 'logging.StreamHandler',
            'formatter': 'verbose_color'
        },
        'console_message': {
            'class': 'logging.StreamHandler',
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
    name = f.__name__
    def _(self):
        local = self.local
        if name not in local:
            setattr(local, name, f(self))
        return getattr(local, name)
    return _
    return property(_, doc=f.__doc__)

def local_property(f):
    return property(local_method(f), doc=f.__doc__)

def threadsafe(f):
    def _(self, *args, **kwargs):
        with self.lock:
            return f(self, *args, **kwargs)
    _.__doc__ = f.__doc__
    _.__name__ = f.__name__
    return _
    
    
class LocalMixin(object):
    '''The :class:`LocalMixin` defines "local" attributes which are
removed when pickling the object'''
    @property
    def local(self):
        if not hasattr(self, '_local'):
            self._local = AttributeDictionary()
        return self._local
    
    @local_property
    def lock(self):
        '''A local threading.Lock.'''
        return Lock()
    
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
    if not hasattr(p,'_pulsar_globals'):
        p._pulsar_globals = {}
    if setval:
        p._pulsar_globals[name] = val
    else:
        return p._pulsar_globals.get(name)


class Silence(logging.Handler):
    def emit(self, record):
        pass
    

class LogginMixin(LocalMixin):
    '''A Mixin used throught the library. It provides built in logging object
and utilities for pickle.'''    
    @property
    def logger(self):
        return self.local.logger
    
    def __setstate__(self, state):
        self.__dict__ = state
        info = getattr(self, '_log_info', {})
        self.configure_logging(**info)
    
    def configure_logging(self, logger=None, config=None, level=None,
                          handlers=None):
        '''Configure logging. This function is invoked every time an
instance of this class is un-serialised (possibly in a different
process domain).'''
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