'''\
A Python implementation of JSON-RPC 2.0 Specification

JSON-RPC is a lightweight remote procedure call protocol.
It's designed to be simple.

A remote method is invoked by sending a request to a remote service.
The request is a single object serialized using JSON.

The specification is at http://groups.google.com/group/json-rpc/web/json-rpc-2-0
'''
import logging
import json

import pulsar
from pulsar.utils.tools import gen_unique_id
from pulsar.http import HttpClient
from pulsar.http.utils import to_string
from pulsar.utils.jsontools import DefaultJSONEncoder, DefaultJSONHook

from .handlers import RpcHandler
from .exceptions import exception, INTERNAL_ERROR


__all__ = ['JSONRPC',
           'JsonProxy',
           'JsonServer']


class JSONRPCException(Exception):
    code = None
    msg  = ''
    def __init__(self, data = None):
        super(JSONRPCException,self).__init__(self.msg)
        self.data = data
        
    def dumps(self):
        return {'code':  self.code,
                'message': self.message,
                'data': self.data}
            
            
class JsonParseError(JSONRPCException):
    code = -32700
    msg = 'Parse error'
    

class JsonRpcInvalidParams(JSONRPCException):
    code = -32602
    msg = 'Invalid params'
    
    
class JsonRpcInternalError(JSONRPCException):
    code = -32603
    msg = 'Internal error'
    

class JsonRpcAuthenticationRequired(JSONRPCException):
    code = -32040
    msg = 'Authentication Required'


class JsonToolkit(object):
    
    @classmethod
    def dumps(cls, data, **kwargs):
        return json.dumps(data, cls=DefaultJSONEncoder, **kwargs)
    
    @classmethod
    def loads(cls, content):
        return json.loads(to_string(content), object_hook=DefaultJSONHook)


class JSONRPC(RpcHandler):
    '''Base class for JSON-RPC services.
Design to comply with the `JSON-RPC 2.0`_ Specification.

.. _`JSON-RPC 2.0`: http://groups.google.com/group/json-rpc/web/json-rpc-2-0'''
    content_type = 'text/json'
    _json = JsonToolkit
        
    def get_method_and_args(self, data):
        req     = self._json.loads(data)
        method  = req.get('method',None)
        params  = req.get('params',None)
        id      = req.get('id',None)
        version = req.get('jsonrpc',None)
        kwargs  = {}
        args    = ()
        if isinstance(params,dict):
            for k,v in params.items():
                kwargs[str(k)] = v
        elif params:
            args = tuple(params)
        return method, args, kwargs, id, version
    
    def dumps(self, id, version, result = None, error = None):
        '''Modify JSON dumps method to comply with
        JSON-RPC Specification 1.0 and 2.0
        '''
        res = {'id': id, "jsonrpc": version}
        
        if error:
            res['error'] = {'code':  getattr(error,'faultCode',INTERNAL_ERROR),
                            'message': str(error),
                            'data': getattr(error,'data','')}
        else:
            res['result'] = result
            
        return self._json.dumps(res)
    
    def serve(self, request):
        content = request._get_raw_post_data()
        if request.method == 'POST':
            method, args, kwargs, id, version = self.get_method_and_args(content)
            meth = self._getFunction(method)
            try:
                res = meth(self,request,*args,**kwargs)
                result = self.dumps(id,version,result=res)
            #except AuthenticationException:
            #    result = self.dumps(id,version,error=JsonRpcAuthenticationRequired())
            except JsonRpcInternalError as e:
                result = self.dumps(id,version,error=e)
            except Exception as e:
                error = JsonRpcInvalidParams(data = str(e))
                result = self.dumps(id,version,error=error)
            
            return self.http.HttpResponse(result,'application/javascript')
        else:
            return self.http.HttpResponse('<h1>Not Found</h1>')


class JsonServer(JSONRPC):
    '''Add Four calls to the base Json Rpc handler.'''
    def rpc_ping(self, request):
        '''Ping the server'''
        return 'pong'
    
    def rpc_server_info(self, request, full = False):
        '''Dictionary of information about the server'''
        worker = request.environ['pulsar.worker']
        info = worker.proxy.info(worker.arbiter, full = full)
        return info.add_callback(lambda res : self.extra_server_info(request, res))
    
    def rpc_functions_list(self, request):
        return list(self.listFunctions())
    
    def rpc_shut_down(self, request):
        request.environ['pulsar.worker'].shut_down()
    
    def extra_server_info(self, request, info):
        '''Add additional information to the info dictionary.'''
        return info
    
    
Handle = JSONRPC


class JsonProxy(object):
    '''A python Proxy class for JSONRPC Servers. Usage::
    
    >>> a = JsonProxy('http://domain.name.com/')
    >>> a.add(3,4)
    7
    
:param url: server location
:param version: JSONRPC server version. Default ``2.0``
:param id: optional request id, generated if not provided. Default ``None``.
:param data: Extra data to include in all requests. Default ``None``.
:param http: optional http opener. If provided it must have the ``request`` method available
             which must be of the form::
             
                 http.request(url, body=..., method=...)
                 
            Default ``None``.
    '''
    separator  = '.'
    rawprefix  = 'raw'
    default_version = '2.0'
    default_timeout = 3
    _json = JsonToolkit
    
    def __init__(self, url, name = None, version = None,
                 proxies = None, id = None, data = None,
                 http = None, timeout = None):
        self.__url     = url
        self.__name    = name
        self.__version = version or self.__class__.default_version
        self.__id      = id
        self.__data    = data if data is not None else {}
        if not http:
            timeout = timeout if timeout is not None else self.default_timeout
            self._http    = HttpClient(proxy_info = proxies,
                                       timeout = timeout)
        else:
            self._http    = http
    
    def __get_path(self):
        return self.__name
    path = property(__get_path)
        
    def makeid(self):
        '''
        Can be re-implemented by your own Proxy
        '''
        return gen_unique_id()
        
    def __str__(self):
        return self.__repr__()
    
    def __repr__(self):
        if self.__id:
            d = '%s - %s' % (self.__url,self.__id)
        else:
            d = self.__url
        return 'JSONRPCProxy(%s)' % d

    def __getattr__(self, name):
        if self.__name != None:
            name = "%s%s%s" % (self.__name, self.separator, name)
        id = self.makeid()
        return self.__class__(self.__url,
                              name = name,
                              version = self.__version,
                              http = self._http,
                              id = id,
                              data = self.__data)

    def __call__(self, *args, **kwargs):
        func_name = self.__name
        fs        = func_name.split('_')
        raw       = False
        if len(fs) > 1 and fs[0] == self.rawprefix:
            raw = True
            fs.pop(0)
            func_name = '_'.join(fs)
            
        params = self.get_params(*args, **kwargs)
        data = {'method':  func_name,
                'params':  params, 
                'id':      self.__id}
        if self.__version:
            data['jsonrpc'] = self.__version
        body = self._json.dumps(data)
        try:
            resp = self._http.request(self.__url,
                                      method = "POST",
                                      body = body)
        except self._http.URLError as e:
            raise pulsar.ConnectionError(str(e))
        if resp.status == 200:
            if raw:
                return resp.content
            else:
                return self.loads(resp.content)
        else:
            raise IOError(resp.reason)
        
    def get_params(self, *args, **kwargs):
        '''
        Create an array or positional or named parameters
        Mixing positional and named parameters in one
        call is not possible.
        '''
        kwargs.update(self.__data)
        if args and kwargs:
            raise ValueError('Cannot mix positional and named parameters')
        if args:
            return list(args)
        else:
            return kwargs
    
    def loads(self, obj):
        res = self._json.loads(obj)
        if isinstance(res, dict):
            if 'error' in res:
                error = res['error']
                code    = error.get('code',None)
                message = error.get('message',None)
                raise exception(code, message)
            else:
                return res.get('result',None)
        return res
    