import sys

SERVER_NAME = 'Pulsar'


__all__ = ['SERVER_NAME',
           'getLogger',
           'LogginMixin',
           'PickableMixin',
           'LogSelf',
           'logerror']


def getLogger(name = None):
    '''Get logger name in "Pulsar" namespace'''
    import logging
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


class LogginMixin(object):
    
    _class_code = None
        
    def getLogger(self, **kwargs):
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
    
    def configure_logging(self):
        pass

    
    
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
        