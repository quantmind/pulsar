import sys
from time import sleep

from .exceptions import AlreadyCalledError, AlreadyRegistered, NotRegisteredWithServer
from .crypt import gen_unique_id

EMPTY_TUPLE = ()
EMPTY_DICT = {}


__all__ = ['Deferred',
           'Remote',
           'RemoteServer',
           'RemoteRequest',
           'RemoteProxyRequest',
           'RemoteProxy',
           'RemoteProxyServer',
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
        self.paused = 0
        self.rid = rid
        self._callbacks = []
    
    @property
    def called(self):
        return self._called
    
    def pause(self):
        """Stop processing until :meth:`unpause` is called.
        """
        self.paused += 1


    def unpause(self):
        """
        Process all callbacks made since L{pause}() was called.
        """
        self.paused -= 1
        if self.paused:
            return
        if self.called:
            self._run_callbacks()
    
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
            while callbacks:
                callback = callbacks.pop(0)
                try:
                    self._runningCallbacks = True
                    try:
                        self.result = callback(self.result)
                    finally:
                        self._runningCallbacks = False
                    if isinstance(self.result, Deferred):
                        self.pause()
                        self.result.add_callback(self._continue)
                except:
                    pass
                
        return self
    
    def _continue(self, result):
        self.result = result
        self.unpause()
    
    def callback(self, result):
        if isinstance(result,Deferred):
            raise ValueError('Received a deferred instance from callback function')
        if self.called:
            raise AlreadyCalledError
        self.result = result
        self._called = True
        self._run_callbacks()
        
    def wait(self, timeout = 1):
        '''Wait until result is available'''
        while not self.called:
            sleep(timeout)
        if isinstance(self.result,Deferred):
            return self.result.wait(timeout)
        else:
            return self.result


def make_deferred(val = None):
    if not isinstance(val,Deferred):
        d = Deferred()
        d.callback(val)
        return d
    else:
        return val
    
                
class RemoteRequest(Deferred):
    LIVE_REQUESTS = {}
    
    def __init__(self, proxyid, name, args = None, kwargs = None, rid = None, ack = True):
        super(RemoteRequest,self).__init__(rid = rid or gen_unique_id())
        self.proxyid = proxyid
        self.name = name
        self.args = args or EMPTY_TUPLE
        self.kwargs = kwargs or EMPTY_DICT
        self.ack = ack
        if ack:
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
        
    @classmethod
    def CallBack(cls, rid, result):
        req = cls.LIVE_REQUESTS.pop(rid,None)
        if req is None:
            raise AlreadyCalledError('Callback {0} already called or never existed'.format(rid))
        req.callback(result)
            

class RemoteProxyRequest(object):
    __slots__ = ('connection','_func_name','proxyid','ack')
    
    def __init__(self, proxyid, name, connection, ack = True):
        self.proxyid = proxyid
        self.connection = connection
        self._func_name = name
        self.ack = ack
        
    def __call__(self, *args, **kwargs):
        request = RemoteRequest(self.proxyid,self._func_name,args,kwargs,ack=self.ack)
        self.connection.send(request)
        return request
    

class RemoteMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(RemoteMetaClass, cls).__new__
        if attrs.pop('virtual',None):
            return make(cls,name,bases,attrs)
        
        fprefix = 'remote_'
        attrib  = '{0}functions'.format(fprefix)
        cont = {}
        for key, method in list(attrs.items()):
            if hasattr(method,'__call__') and key.startswith(fprefix):
                name = key[len(fprefix):]
                method = attrs.pop(key)
                method.__name__ = name
                cont[name] = method
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs[attrib] = cont
        attrs['remotes'] = dict((name,getattr(attr,'ack',True)) for name,attr in cont.items())
        return make(cls, name, bases, attrs)

    
BaseRemote = RemoteMetaClass('BaseRemote',(object,),{})


class RemoteProxyServer(object):
    '''A proxy for a remote object used by processes to call
remote functions via a duplex connection.
To specify a functions called by remote objects prefix with "proxy_".

.. attribute:: remotes

    Tuple of remote functions names. Default ``()``.
    
'''
    REQUEST = RemoteProxyRequest
    
    def __init__(self, connection, log = None):
        self.connection = connection
        self.REGISTERED_REMOTES = {}
        self.log = log
    
    def send(self, data):
        if isinstance(data,Remote):
            data = data.get_remote()
            
    def close(self):
        '''Close the proxy by closing the duplex connection.'''
        self.connection.close()
    
    def flush(self):
        '''Flush the pipe and runs callbacks'''
        c = self.connection
        while c.poll():
            request = c.recv()
            try:
                pid = request.proxyid
                if pid in self.REGISTERED_REMOTES:
                    obj = self.REGISTERED_REMOTES[pid]
                    obj.handle_remote_request(request)
                else:
                    RemoteRequest.CallBack(request.rid,request.result)
            except Exception as e:
                result = e
                if self.log:
                    self.log.error('Error while processing worker request: {0}'.format(e),
                                    exc_info=sys.exc_info())
            

class RemoteProxy(object):
    REQUEST = RemoteProxyRequest
    
    def __init__(self, proxyid, remotes, connection = None):
        self.proxyid = proxyid
        self.remotes = remotes
        self.connection = connection
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return self.REQUEST(self.proxyid,name, self.connection, ack = ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))
        
    
    
class Remote(BaseRemote):
    '''A proxy for a remote object used by processes to call
remote functions via a duplex connection.
To specify a functions called by remote objects prefix with "proxy_".

.. attribute:: remotes

    Tuple of remote functions names. Default ``()``.
    
'''
    _server = None
    
    @property
    def proxyid(self):
        if hasattr(self,'_proxyid'):
            return self._proxyid
    
    def register_with_server(self, server):
        if self._server:
            raise AlreadyRegistered('{0} already registered with a remote server')
        if isinstance(server,RemoteProxyServer):
            self._server = server
            pid = gen_unique_id()
            server.REGISTERED_REMOTES[pid] = self
            self._proxyid = pid
        return self
    
    def get_proxy(self, remote_connection):
        '''Return an instance of :class:`RemoteProxy` pointig to self. The return object
can be passed to remote process.'''
        pid = self.proxyid
        if not pid:
            raise NotRegisteredWithServer
        if isinstance(remote_connection,RemoteProxyServer):
            remote_connection = remote_connection.connection
        return RemoteProxy(pid, self.remotes, remote_connection)
    
    def handle_remote_request(self, request):
        func = self.remote_functions[request.name]
        result = func(self, *request.args, **request.kwargs)
        if request.ack:
            self.run_remote_callback(request,result)
                
    def run_remote_callback(self, response, result):
        '''Run the remote callback.'''
        if isinstance(result,Remote):
            result = result.get_proxy()
        response.callback(result)
        self._server.connection.send(response)

    
class RemoteServer(RemoteProxyServer,Remote):
    '''A remote object which is also a server'''
    def __init__(self, *args, **kwargs):
        super(RemoteServer,self).__init__(*args, **kwargs)
        self.register_with_server(self)
        
