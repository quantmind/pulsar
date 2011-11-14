import sys
import copy
from time import time
import logging
from multiprocessing import current_process, Lock

if sys.version_info < (2,7):
    from pulsar.utils.fallbacks.dictconfig import dictConfig
else:
    from logging.config import dictConfig
    

SERVER_NAME = 'Pulsar'
NOLOG = 100


__all__ = ['SERVER_NAME',
           'getLogger',
           'process_global',
           'LogginMixin',
           'Synchronized',
           'LocalMixin',
           'LogSelf',
           'LogInformation']


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
            'format': '%(asctime)s [p=%(process)s,t=%(thread)s]\
 [%(levelname)s] [%(name)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'verbose_color': {
            '()': 'pulsar.utils.tools.ColorFormatter',
            'format': '%(asctime)s [p=%(process)s,t=%(thread)s]\
 [%(levelname)s] [%(name)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(asctime)s %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'silent': {
            'class': 'pulsar.utils.log.Silence',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose_color'
        }
    },
    'filters ': {},
    'loggers': {},
    'root': {}
}


def update_config(config, c):
    for name in ('handlers','formatters','filters','loggers','root'):
        if name in c:
            config[name].update(c[name])


class LocalMixin(object):
    
    @property
    def local(self):
        if not hasattr(self,'_local'):
            self._local = {}
        return self._local
     
    def __getstate__(self):
        '''Remove the local dictionary.'''
        d = self.__dict__.copy()
        d.pop('_local',None)
        return d
    
    
class SynchronizedMixin(object):
    
    @property
    def lock(self):
        if '_lock' not in self.local:
            self.local['_lock'] = Lock()
        return self.local['_lock']
    
    @classmethod
    def make(cls, f):
        """Synchronization decorator for Synchronized member functions. """
    
        def _(self, *args, **kw):
            lock = self.lock
            lock.acquire()
            try:
                return f(self, *args, **kw)
            finally:
                lock.release()
                
        _.thread_safe = True
        _.__doc__ = f.__doc__
        
        return _
    
    
class Synchronized(LocalMixin,SynchronizedMixin):
    pass


def process_global(name, val = None, setval = False):
    '''Access and set global variables for the current process.'''
    p = current_process()
    if not hasattr(p,'_pulsar_globals'):
        p._pulsar_globals = {}
    if setval:
        p._pulsar_globals[name] = val
    else:
        return p._pulsar_globals.get(name)


def getLogger(name = None):
    '''Get logger name in "pulsar" namespace'''
    prefix = SERVER_NAME.lower() + '.'
    name = name or ''
    if not name.startswith(prefix):
        name = prefix + name
    return logging.getLogger(name)


class LogSelf(object):
    '''\
    Wrapper for logging with the message starting with the
string representation of an instance.

:parameter instance: instance which prefix the message.
:parameter logger: the logger object.
    '''
    LOGGING_FUNCTIONS = ('debug','info','error','warning','warn',
                         'critical','exception')
    
    def __init__(self,instance,logger):
        self.instance = instance
        self.logger = logger
        for func in self.LOGGING_FUNCTIONS:
            setattr(self,func,self._handle(func))
    
    @property
    def name(self):
        return self.logger.name
    
    def _msg(self, msg):
        return '{0} - {1}'.format(self.instance,msg)
    
    def _handle(self, name):
        func = getattr(self.logger,name)
        def _(msg, *args, **kwargs):
            func(self._msg(msg),*args,**kwargs)
        _.__name__ = name
        return _


class Silence(logging.Handler):
    def emit(self, record):
        pass
    

class LogginMixin(Synchronized):
    '''A Mixin used throught the library. It provides built in logging object
and utilities for pickle.'''
    loglevel = None
    default_logging_level = None
    default_logging_config = None
    _class_code = None
    
    def setlog(self, log = None, **kwargs):
        if not log:
            name = getattr(self,'_log_name',self.class_code)
            log = getLogger(name)
        self.local['log'] = log
        self._log_name = log.name
        return log
        
    @property
    def log(self):
        return self.local.get('log')      
    
    def __repr__(self):
        return self.class_code
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def class_code(self):
        return self.__class__.code()
    
    @classmethod
    def code(cls):
        return cls._class_code or cls.__name__.lower()
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.configure_logging()
    
    def configure_logging(self, config = None):
        '''Configure logging. This function is invoked every time an
instance of this class is unserialized (possibly in a different process domain).
'''
        logconfig = None
        if not process_global('_config_logging'):
            logconfig = copy.deepcopy(LOGGING_CONFIG)
            if config:
                update_config(logconfig,config)
            process_global('_config_logging',logconfig,True)
            
        loglevel = self.loglevel
        if loglevel is None:
            loglevel = logging.NOTSET
        else:
            try:
                loglevel = int(loglevel)
            except (ValueError):
                lv = str(loglevel).upper()
                if lv in logging._levelNames:
                    loglevel = logging._levelNames[lv]
                else:
                    loglevel = logging.NOTSET
    
        self.loglevel = loglevel

        # No loggers configured. This means no logconfig setting
        # parameter was used. Set up the root logger with default
        # loggers 
        if logconfig:
            if not logconfig.get('root'):
                if loglevel == logging.NOTSET:
                    logconfig['root'] = {
                                    'handlers':['silent']
                                }
                else:
                    logconfig['root'] = {
                                    'level': logging.getLevelName(loglevel),
                                    'handlers':['console']
                                }
            dictConfig(logconfig)
        
        self.setlog()
        self.log.setLevel(self.loglevel)
            
        
class LogInformation(object):
    
    def __init__(self, logevery):
        self.logevery = logevery
        self.last = time()
        
    def log(self):
        if self.logevery:
            t = time()
            if t - self.last > self.logevery:
                self.last = t
                return t
        