import sys
import weakref
from collections import *

if sys.version_info < (2,7):
    from pulsar.utils.fallbacks._collections import *
    
    

class WeakList(object):
    
    def __init__(self):
        self._list = []
        
    def append(self, obj):
        if obj:
            self._list.append(weakref.ref(obj))
        
    def __iter__(self):
        if self._list:
            ol = self._list
            nl = self._list = []
            for v in ol:
                obj = v()
                if obj:
                    nl.append(v)
                    yield obj
        else:
            raise StopIteration
