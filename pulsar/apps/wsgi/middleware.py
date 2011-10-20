
__all__ = ['AccessControl']


class AccessControl(object):
    '''A response middleware which add the ``Access-Control-Allow-Origin``
response header.'''
    def __init__(self, origin = '*', methods = None):
        self.origin = origin
        self.methods = methods
        
    def __call__(self, environ, status, headers):
        if status != '200 OK':
            return status
        headers.append(('Access-Control-Allow-Origin',self.origin))
        if self.methods:
            headers.append(('Access-Control-Allow-Methods', self.methods))
        return status
                       
        
        