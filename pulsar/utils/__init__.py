

class Middleware(object):
    '''A middleware handler. This simple class can add handler
and apply them to an object. It is very general.'''
    
    def __init__(self):
        self.handles = []
        
    def add(self, handle):
        self.handles.append(handle)
        
    def apply(self, elem):
        for handle in self.handles:
            try:
                handle(elem)
            except Exception as e:
                pass


