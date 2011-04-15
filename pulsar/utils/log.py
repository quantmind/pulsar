SERVER_NAME = 'Pulsar'


__all__ = ['SERVER_NAME',
           'getLogger',
           'PickableMixin']


def getLogger(name = None):
    import logging
    name = '{0}.{1}'.format(SERVER_NAME,name) if name else SERVER_NAME
    return logging.getLogger(name)


class PickableMixin(object):
    _class_code = None
     
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('log',None)
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.log = getLogger(self.class_code) 
        self.configure_logging()
        
    def getLogger(self, **kwargs):
        logger = kwargs.pop('logger',None)
        return logger or getLogger(self.class_code)
            
    @property
    def class_code(self):
        return self.__class__.code()
    
    @classmethod
    def code(cls):
        return cls._class_code or cls.__name__
    
    def configure_logging(self):
        pass
