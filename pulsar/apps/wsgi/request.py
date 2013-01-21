__all__ = ['Request']

class Request(object):
    '''Wsgi environment wrapper'''

    def __init__(self, environ):
        self.environ = environ

    def __repr__(self):
        return self.environ.__repr__()
    
    def __str__(self):
        return self.__repr__()

    def __getattr__(self, name):
        if name in self.environ:
            return self.environ[name]
        elif name in self.__dict__:
            return self.__dict__[name]
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %\
                                 (self.__class__.__name__, name))

    def __getitem__(self, name):
        return self.environ[name]
    
    def __setitem__(self, name, value):
        self.environ[name] = value
        
    def __len__(self):
        return len(self.environ)
    
    def __iter__(self):
        return iter(self.environ)
    