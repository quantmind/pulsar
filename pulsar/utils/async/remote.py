import sys
from time import sleep

from pulsar import AlreadyCalledError, AlreadyRegistered, NotRegisteredWithServer
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred


__all__ = ['RemoteProxyServer',
           'Remote',
           'RemoteServer',
           'RemoteRequest',
           'RemoteProxyRequest',
           'RemoteProxy']


class RemoteRequest(Deferred):
    LIVE_REQUESTS = {}
    
    def __init__(self, proxyid, name, args, kwargs, rid = None, ack = True):
        super(RemoteRequest,self).__init__(rid = rid or gen_unique_id())
        self.proxyid = proxyid
        self.name = name
        self.args = args
        self.kwargs = kwargs
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
        ser = self.serialize
        args = tuple((ser(a) for a in args))
        kwargs = dict((k,ser(a)) for k,a in iteritems(kwargs))
        request = RemoteRequest(self.proxyid,self._func_name,args,kwargs,ack=self.ack)
        self.connection.send(request)
        return request
    
    def serialize(self, obj):
        if isinstance(obj,RemoteProxy):
            return obj.noconnection()
        elif isinstance(obj,Remote):
            return obj.get_proxy()
        else:
            return obj
        

class RemoteMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(RemoteMetaClass, cls).__new__
        fprefix = 'remote_'
        attrib  = '{0}functions'.format(fprefix)
        cont = {}
        for key, method in list(attrs.items()):
            if hasattr(method,'__call__') and key.startswith(fprefix):
                meth_name = key[len(fprefix):]
                method = attrs.pop(key)
                method.__name__ = meth_name 
                cont[meth_name] = method
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs[attrib] = cont
        attrs['remotes'] = dict((meth_name ,getattr(attr,'ack',True))\
                                 for meth_name ,attr in cont.items())
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
        self._connection = connection
        self.REGISTERED_REMOTES = {}
        self.log = log
    
    @property
    def connection(self):
        return self._connection
    
    def send(self, data):
        if isinstance(data,Remote):
            data = data.get_remote()
            
    def close(self):
        '''Close the server by closing the duplex connection.'''
        self.connection.close()
    
    def flush(self):
        '''Flush the pipe and runs callbacks'''
        c = self.connection
        while c.poll():
            request = c.recv()
            obj = None
            try:
                pid = request.proxyid
                if pid in self.REGISTERED_REMOTES:
                    obj = self.REGISTERED_REMOTES[pid]
                    obj.handle_remote_request(request)
                else:
                    RemoteRequest.CallBack(request.rid,request.result)
            except Exception as e:
                if obj:
                    obj.run_remote_callback(request,e)
                if self.log:
                    self.log.error('Error while processing worker request: {0}'.format(e),
                                    exc_info=sys.exc_info())
    
    def unserialize(self, obj):
        '''Unserialize once the request has arrived to the remote server.'''
        if isinstance(obj,RemoteProxy):
            if obj.proxyid in self.REGISTERED_REMOTES:
                return self.REGISTERED_REMOTES[obj.proxyid]
            else:
                obj.connection = self.connection
                return obj
        else:
            return obj


class RemoteProxy(object):
    __slots__ = ('proxyid','remotes','connection')
    
    def __init__(self, proxyid, remotes, connection = None):
        self.proxyid = proxyid
        self.remotes = remotes
        self.connection = connection
        
    def noconnection(self):
        return self.__class__(self.proxyid,self.remotes)
    
    def __getstate__(self):
        '''Because of the __getattr__ implementation,
we need to manually implement the pcikling and unpickling of thes object'''
        return (self.proxyid,self.remotes,self.connection)
    
    def __setstate__(self, state):
        self.proxyid,self.remotes,self.connection = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return RemoteProxyRequest(self.proxyid,name, self.connection, ack = ack)
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
    def server(self):
        return self._server
    
    @property
    def proxyid(self):
        if hasattr(self,'_proxyid'):
            return self._proxyid
        
    @property
    def connection(self):
        if self._server:
            return self._server.connection
        
    def __reduce__(self):
        return (RemoteProxy,(self.proxyid,self.remotes))
        
    def register_with_server(self, server):
        if self._server:
            raise AlreadyRegistered('{0} already registered with a remote server')
        if isinstance(server,RemoteProxyServer):
            self._server = server
            pid = gen_unique_id()
            server.REGISTERED_REMOTES[pid] = self
            self._proxyid = pid
        return self
    
    def get_proxy(self, remote_connection = None):
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
        uns = self.server.unserialize
        args = tuple((uns(a) for a in request.args))
        kwargs = dict(((k,uns(a)) for k,a in iteritems(request.kwargs)))
        result = func(self, *args, **kwargs)
        self.run_remote_callback(request,result)
        
    def run_remote_callback(self, request, result):
        if request.ack:
            self._run_remote_callback(request, result)
                
    def _run_remote_callback(self, response, result):
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
        
