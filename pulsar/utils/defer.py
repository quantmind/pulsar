import sys
from .exceptions import AlreadyCalledError
from .crypt import gen_unique_id

EMPTY_TUPLE = ()
EMPTY_DICT = {}


__all__ = ['Deferred',
           'Remote',
           'RemoteRequest',
           'RemoteProxyRequest',
           'RemoteProxy',
           'RemoteProxyObject']



class Deferred(object):
    """
    This is a callback which will be put off until later. The idea is the same
    as twisted.defer.Deferred object.

    Use this class to return from functions which otherwise would block the
    program execution. Instead, it should return a Deferred.
    """
    def __init__(self, rid = None):
        self._called = False
        self.rid = rid
        self._callbacks = []
    
    @property
    def called(self):
        return self._called
    
    def add_callback(self, callback):
        """Add a callback as a callable function. The function takes one argument,
the result of the callback.
        """
        self._callbacks.append(callback)
        self._run_callbacks()
        return self
        
    def _run_callbacks(self):
        if self._called and self._callbacks:
            callbacks = self._callbacks
            self._callbacks = []
            for callback in callbacks:
                callback(self.result)
        return self
    
    def callback(self, result):
        if isinstance(result,Deferred):
            raise ValueError('Received a deferred instance from callback function')
        if self.called:
            raise AlreadyCalledError
        self.result = result
        self._called = True
        self._run_callbacks()


class RemoteRequest(Deferred):
    LIVE_REQUESTS = {}
    
    def __init__(self, name, args = None, kwargs = None, ack = True, rid = None):
        super(RemoteRequest,self).__init__(rid = rid or gen_unique_id())
        self.name = name
        self.args = args or EMPTY_TUPLE
        self.kwargs = kwargs or EMPTY_DICT
        self.ack = ack
        self.LIVE_REQUESTS[self.rid] = self
        
    def __str__(self):
        return '{0} - {1}'.format(self.name,self.rid[:8])
    
    def __repr__(self):
        return self.__str__()
    
    def __getstate__(self):
        #Remove the list of callbacks
        d = self.__dict__.copy()
        d['_callbacks'] = []
        return d
        
    def callback(self, result):
        super(RemoteRequest,self).callback(result)
        self.ack = False
        
    @classmethod
    def CallBack(cls, rid, result):
        req = cls.LIVE_REQUESTS.pop(rid,None)
        if req is None:
            raise AlreadyCalledError('Callback {0} already called or never existed'.format(rid))
        req.callback(result)
            

class RemoteProxyRequest(object):
    __slots__ = ('connection','_func_name','ack')
    
    def __init__(self, connection, name, ack):
        self.connection = connection
        self._func_name = name
        self.ack = ack
        
    def __call__(self, *args, **kwargs):
        request = RemoteRequest(self._func_name,args,kwargs,self.ack)
        self.connection.send(request)
        return request
    

class RemoteProxyMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(RemoteProxyMetaClass, cls).__new__
        if attrs.pop('virtual',None):
            return make(cls,name,bases,attrs)
        
        fprefix = 'proxy_'
        cont = {}
        for key, method in list(attrs.items()):
            if hasattr(method,'__call__') and key.startswith(fprefix):
                name = key[len(fprefix):]
                method = attrs.pop(key)
                method.__name__ = name
                cont[name] = method
            for base in bases[::-1]:
                if hasattr(base, 'proxy_functions'):
                    rbase = base.proxy_functions
                    for key,method in rbase.items():
                        if not cont.has_key(key):
                            cont[key] = method
                        
        attrs['proxy_functions'] = cont
        return make(cls, name, bases, attrs)

    
BaseRemoteProxy = RemoteProxyMetaClass('BaseRemoteProxy',(object,),{})


class Remote(object):
    REMOTE_PROXIES = {}
    remotes = ()
    def get_proxy(self, rid, connection):
        proxy = RemoteProxy(connection, remotes = self.remotes, rid = rid)
        self.REMOTE_PROXIES[rid] = self
        return proxy
    
    
class RemoteProxy(BaseRemoteProxy):
    '''A proxy for a remote object used by processes to call
remote functions via a duplex connection.
To specify a functions called by remote objects prefix with "proxy_".

.. attribute:: remotes

    Tuple of remote functions names. Default ``()``.
    
'''
    remotes = ()
    REQUEST = RemoteProxyRequest
    
    def __init__(self, connection, log = None, remotes = None, rid = None):
        self.connection = connection
        self.rid = rid
        if remotes:
            self.remotes = remotes
        self.log = log
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('log',None)
        return d
    
    def close(self):
        '''Close the proxy by closing the duplex connection.'''
        self.connection.close()

    def callback(self, response, result):
        '''Got a result for request. If required runs callbacks
        Callback the caller.'''
        if response.ack:
            if isinstance(result,Remote):
                result = result.get_proxy(response.rid, self.connection)
            response.callback(result)
            self.connection.send(response)
    
    def __getattr__(self, name):
        if name in self.remotes:
            ack = name in self.proxy_functions
            return self.REQUEST(self.connection,name,ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))
    
    def flush(self):
        '''Flush the pipe and runs callbacks'''
        c = self.connection
        while c.poll():
            request = c.recv()
            try:
                func = self.proxy_functions[request.name]
                if hasattr(request,'result'):
                    func(self, request)
                    RemoteRequest.CallBack(request.rid,request.result)
                else:
                    result = func(self, *request.args, **request.kwargs)
                    self.callback(request,result)
            except Exception as e:
                result = e
                if self.log:
                    self.log.error('Error while processing worker request: {0}'.format(e),
                                    exc_info=sys.exc_info())
            

class RemoteProxyObject(RemoteProxy):
    
    def __init__(self, obj, connection, log = None):
        self.obj = obj
        super(RemoteProxyObject,self).__init__(connection,log=log)
    