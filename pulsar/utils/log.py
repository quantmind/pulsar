import sys
import logging

from .system import platform

SERVER_NAME = 'Pulsar'


__all__ = ['SERVER_NAME',
           'getLogger',
           'LogginMixin',
           'PickableMixin',
           'Silence',
           'LogSelf',
           'logerror']


LOG_LEVELS = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG
    }


def getLogger(name = None):
    '''Get logger name in "Pulsar" namespace'''
    name = '{0}.{1}'.format(SERVER_NAME,name) if name else SERVER_NAME
    return logging.getLogger(name)


def logerror(func):
    
    def _(self,*args,**kwargs):
        try:
            return func(self,*args,**kwargs)
        except Exception as e:
            if self.log:
                self.log.critical('"{0}" had an unhandled exception in function "{1}": {2}'\
                                  .format(self,func.__name__,e),exc_info=sys.exc_info())
            pass
        
    return _


class LogSelf(object):
    '''\
    Wrapper for logging with the message starting with the
string representation of an instance.

:parameter instance: instance which prefix the message.
:parameter logger: the logger object.
    '''
    LOGGING_FUNCTIONS = ('debug','info','error','warning','critical','exception')
    
    def __init__(self,instance,logger):
        self.instance = instance
        self.logger = logger
        for func in self.LOGGING_FUNCTIONS:
            setattr(self,func,self._handle(func))
    
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
    

class LogginMixin(object):
    loglevel = None
    default_logging_level = None
    _class_code = None
        
    def getLogger(self, **kwargs):
        if hasattr(self,'log'):
            return self.log
        else:
            logger = kwargs.pop('logger',None)
            return logger or getLogger(self.class_code)
    
    def __repr__(self):
        return self.class_code
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def class_code(self):
        return self.__class__.code()
    
    @classmethod
    def code(cls):
        return cls._class_code or cls.__name__
    
    def configure_logging(self, handlers = None):
        '''Configure logging'''
        loglevel = self.loglevel
        try:
            self.loglevel = int(loglevel)
        except (TypeError,ValueError):
            lv = str(loglevel).lower()
            self.loglevel = LOG_LEVELS.get(lv,self.default_logging_level)
        logger = logging.getLogger()
        color = False
        if not handlers:
            handlers = []
            if self.loglevel is None:
                handlers.append(Silence())
            else:
                color = True
                handlers.append(logging.StreamHandler())
        f = self.logging_formatter(color)
        for h in handlers:
            h.setFormatter(f)
            logger.addHandler(h)
            if self.loglevel is not None:
                logger.setLevel(self.loglevel)

    def logging_formatter(self, color = False):
        format = '%(asctime)s [p=%(process)s,t=%(thread)s] [%(levelname)s] [%(name)s] %(message)s'
        #format = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
        datefmt = r"%Y-%m-%d %H:%M:%S"
        if color and not platform.isWindows():
            from pulsar.utils.tools import ColorFormatter as Formatter
        else:
            Formatter = logging.Formatter
        return Formatter(format, datefmt)
    
    
class PickableMixin(LogginMixin):
    '''A Mixin used throught the library. It provides built in logging object and
utilities for pickle.'''
    REMOVABLE_ATTRIBUTES = ()
     
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('log',None)
        for attr in self.REMOVABLE_ATTRIBUTES:
            d.pop(attr,None)
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.log = getLogger(self.class_code) 
        self.configure_logging()
        