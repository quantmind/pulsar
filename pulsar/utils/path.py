'''Stand alone compact module for managing python paths.'''
import os
import sys

if sys.version_info > (3,0):
    string_type = str
else:   #pragma    nocover
    string_type = unicode
    range = xrange

__all__ = ['Path']


class Path(string_type):

    def __new__(cls, path=None):
        path = path or ''
        abspath = os.path.abspath(path)
        return super(Path,cls).__new__(cls, abspath)

    @classmethod
    def cwd(cls):
        '''Return the current working directory as a path object.'''
        return cls(os.getcwd())
        
    isfile = lambda self: os.path.isfile(self)
    isdir = lambda self: os.path.isdir(self)
    exists = lambda self: os.path.exists(self)
    realpath = lambda self: self.__class__(os.path.realpath(string_type(self)))
        
    def join(self, *path):
        return self.__class__(os.path.join(self, *path))
    
    def split(self):
        d,f = os.path.split(self)
        return self.__class__(d),f
    
    def dir(self):
        if self.isfile():
            return self.parent
        elif self.isdir():
            return self
        else:
            raise ValueError('%s not a valid directory' % self)
        
    @property
    def parent(self):
        return self.__class__(os.path.dirname(self))
        
    def ancestor(self, n):
        p = self
        for i in range(n):
            p = p.parent
        return p
        
    def ispymodule(self):
        if self.isdir():
            return os.path.isfile(os.path.join(self, '__init__.py'))
        elif self.isfile():
            return self.endswith('.py')
             
    def add2python(self, module=None, up=0, down=None, front=False,
                   must_exist=True):
        '''Add a directory to the python path.
        
:parameter module: Optional module name to try to import once we have found
    the directory
:parameter up: number of level to go up the directory three from
    :attr:`local_path`.
:parameter down: Optional tuple of directory names to travel down once we have
    gone *up* levels.
:parameter front: Boolean indicating if we want to insert the new path at the
    front of ``sys.path`` using ``sys.path.insert(0,path)``.
:parameter must_exist: Boolean indicating if the module must exists.'''
        if module:
            try:
                __import__(module)
                return module
            except ImportError:
                pass
        
        dir = self.dir().ancestor(up)
        if down:
            dir = dir.join(*down)
        added = False
        if dir.isdir():
            if dir not in sys.path:
                if front:
                    sys.path.insert(0, dir)
                else:
                    sys.path.append(dir)
                added = True
        else:
            raise ValueError('Directory {0} not available'.format(dir))
        if module:
            try:
                __import__(module)
                return module
            except ImportError:
                if must_exist:
                    raise
