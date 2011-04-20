import sys
import time
from multiprocessing import Process, reduction
from threading import Thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   NotRegisteredWithServer, PickableMixin,\
                   logerror
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred


__all__ = ['is_remote',
           'get_proxy',
           'RemoteMixin',
           'Remote',
           'RemoteProxy',
           'RemoteServer',
           'RemoteController',
           'ProcessWithRemote']


def is_remote(obj):
    return isinstance(obj,RemoteMixin)

def get_proxy(obj):
    if is_remote(obj):
        return obj.get_proxy()
    elif isinstance(obj,RemoteProxy):
        return obj
    else:
        raise ValueError('"{0}" is not a remote or remote proxy.'.format(obj))


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
        c = self.connection
        if not c:
            raise ValueError('No Connection. Check implementation')
        ser = self.serialize
        args = tuple((ser(a) for a in args))
        kwargs = dict((k,ser(a)) for k,a in iteritems(kwargs))
        request = RemoteRequest(self.proxyid,self._func_name,args,kwargs,ack=self.ack)
        c.send(request)
        return request
    
    def serialize(self, obj):
        if is_remote(obj):
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


class RemoteProxy(object):
    '''A proxy for a :class:`pulsar.utils.async.RemoteMixin` instance.
This is a light class which delegates function calls to a remote object.

.. attribute: proxyid

    Unique ID for the remote object
    
.. attribute: remotes

    dictionary of remote functions names with value indicationg if the
    remote function will acknowledge the call.
'''    
    __slots__ = ('proxyid','remotes','connection')
    
    def __init__(self, proxyid, remotes, connection = None):
        self.proxyid = proxyid
        self.remotes = remotes
        self.connection = connection
    
    def __repr__(self):
        return self.proxyid[:8]
    
    def __str__(self):
        return self.__repr__()
    
    def __getstate__(self):
        '''Because of the __getattr__ implementation,
we need to manually implement the pcikling and unpickling of thes object.
Furthermore, the connection is not serialized.'''
        return (self.proxyid,self.remotes,None)
    
    def __setstate__(self, state):
        self.proxyid,self.remotes,self.connection = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return RemoteProxyRequest(self.proxyid,name, self.connection, ack = ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))
    
    
class RemoteMixin(BaseRemote):
    '''Mixin for classes exposing functions which
can be used on separate process. Before a :class:`RemoteMixin` can be
accessed it must be registered with a :class:`RemoteServer` instance.
Functions called by remote objects must be prefixed with ``remote_``.

.. attribute:: remotes

    Tuple of remote functions names. This attribute is
    filled by the metaclass.
    
.. attribute:: proxyid

    Unique id for the Remote object. Available when ``self`` is registered
    with a :class:`pulsar.utils.async.RemoteServer`.
'''
    _remote_server = None
    
    @property
    def proxyid(self):
        if hasattr(self,'_proxyid'):
            return self._proxyid
        
    @property
    def remote(self):
        return self._remote_server
        
    def __reduce__(self):
        return (RemoteProxy,(self.proxyid,self.remotes))
    
    def register_with_server(self, server):
        '''Register the ``self`` with a remote server.
If the object is already registered with a server, a :class:`pulsar.AlreadyRegistered`
exception raises.'''
        if self.proxyid:
            raise AlreadyRegistered('{0} already registered with a remote server')
        if isinstance(server,RemoteServer):
            if server is not self:
                self._server = server
            pid = gen_unique_id()
            server.REGISTERED_REMOTES[pid] = self
            self._proxyid = pid
        return self
    
    def register_with_remote(self):
        '''Register ``self`` with a remote server.'''
        proxy = self.get_proxy(self.server)
        return proxy.link(self).add_callback(self._got_remote)
    
    def _got_remote(self,remote):
        self._remote_server = remote
    
    def get_proxy(self, remote_connection = None):
        '''Return :class:`RemoteProxy` instance of ``self``.
If not yet registered with a :class:`RemoteServer` a :class:`pulsar.NotRegisteredWithServer`
exception raises.'''
        pid = self.proxyid
        if not pid:
            raise NotRegisteredWithServer
        if isinstance(remote_connection,RemoteServer):
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
        if is_remote(result):
            result = result.get_proxy()
        response.callback(result)
        self.server.connection.send(response)
    
    def remote_link(self, remote):
        '''Link the controller with a remote object.'''
        self._got_remote(remote)
        return self
    
    def remote_run(self, method, *args, **kwargs):
        '''Run a method in process domain of ``self``.'''
        return method(*args,**kwargs)

    def remote_ping(self):
        '''Just a remote ping function'''
        return 'pong'
    
    
class Remote(RemoteMixin):
    _server = None
    
    @property
    def server(self):
        return self._server
        
    @property
    def connection(self):
        if self._server:
            return self._server.connection


class RemoteServer(RemoteMixin):
    '''A server for object with remote functionality.
Communication with remote objects is obtained via a duplex connection.
'''
    REQUEST = RemoteProxyRequest
    
    def __init__(self, connection, log = None):
        self._connection = self._reduce_connection(connection)
        self.REGISTERED_REMOTES = {}
        self.log = log
        self.register_with_server(self)
    
    def _reduce_connection(self, c):
        # Not sure why we need to do this, but it needs to be done
        return c
        reduced = reduction.reduce_connection(c)
        return reduced[0](*reduced[1])
        
    @property
    def server(self):
        return self
    
    @property
    def connection(self):
        return self._connection
            
    def close(self):
        '''Close the server by closing the duplex connection.'''
        self.connection.close()
    
    @logerror
    def flush(self):
        '''Flush the pipe and runs callbacks. This function should live
on a event loop.'''
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
                    if hasattr(request,'result'):
                        RemoteRequest.CallBack(request.rid,request.result)
                    else:
                        self.handle_remote_request(request)
            except Exception as e:
                if obj:
                    obj.run_remote_callback(request,e)
                self.handle_request_error(request,e)
                if self.log:
                    self.log.error('Error while processing worker request: {0}'.format(e),
                                    exc_info=sys.exc_info())
    
    def handle_request_error(self,request,e):
        pass
    
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
        

class RemoteController(RemoteServer):
    '''A :class:`RemoteServer` with an object acting as 
controller. The object can be an instance of a python :class:`multiprocessing.Process`
or :class:`threading.Thread` class.'''
    def __init__(self, connection, obj, log = None):
        log = log or getattr(obj,'log',None)
        super(RemoteController,self).__init__(connection, log = log)
        self._notified = time.time()
        self._remote = None
        self._obj = obj
    
    def is_alive(self):
        return self._obj.is_alive()
    
    @logerror
    def _got_remote(self,remote):
        self._remote = remote
        self._obj.on_remote(remote)
            
    def __repr__(self):
        return self._obj.__repr__()
    
    def __str__(self):
        return self._obj.__str__()
    
    def remote_stop(self):
        '''Stop the controller'''
        self._obj._stop()
    remote_stop.ack = False
    
    def remote_notify(self, ts):
        '''Receive a notification from a remote.'''
        self._notified = ts
        self._obj._notified()
    remote_notify.ack = False
    
        
class ProcessWithRemote(PickableMixin):
    '''A Remote server mixin to be used as Base class
for python :class:`multiprocessing.Process` or
or :class:`threading.Thread` classes. For example::

    class MyProcessServer(ProcessWithRemote,Process):
        _done = False
        
        def _run(self):
            while not self._done:
                self.flush()
                time.sleep(0.1)
        
        def _stop(self):
            self._done = True
'''
    controller_class = RemoteController
    server = None
    
    def __init__(self, connection):
        self._connection = connection
        
    def init_in_process(self):
        '''Called After forking (if a process)'''
        if not hasattr(self,'log'):
            self.log = self.getLogger()
        self.server = self.controller_class(self._connection,self)
        
    def run(self):
        self.init_in_process()
        self._run()
        
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        raise NotImplementedError

    def _stop(self):
        '''The stop implementation which must be done by a derived class.'''
        raise NotImplementedError
    
    def close(self):
        '''Close the server connection.'''
        if self.server:
            self.server.close()
    
    @classmethod
    def is_thread(cls):
        return issubclass(cls,Thread)
    
    @classmethod
    def is_process(cls):
        return issubclass(cls,Process)
    
    def flush(self):
        remote = self.server._remote
        if remote:
            remote.notify(time.time())
        self.server.flush()

    def _notified(self):
        pass

    def on_remote(self,remote):
        '''Callback when the remote server proxy arrives.'''
        return remote
