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
from timeit import default_timer

import pulsar
from pulsar.utils.tools import gen_unique_id
from pulsar.net import HttpClient
from pulsar.utils.http import to_string
from pulsar.utils.py2py3 import range
from pulsar.utils.jsontools import DefaultJSONEncoder, DefaultJSONHook

from .handlers import RpcHandler
from .exceptions import exception, INTERNAL_ERROR


__all__ = ['JSONRPC','JsonProxy']


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
    #content_type = 'application/javascript'
    methods = ('post',)
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
    

class JsonProxy(object):
    '''A python Proxy class for :class:`JSONRPC` Servers.
    
:param url: server location
:param version: JSONRPC server version. Default ``2.0``
:param id: optional request id, generated if not provided. Default ``None``.
:param data: Extra data to include in all requests. Default ``None``.
:param http: optional http opener. If provided it must have the ``request``
    method available which must be of the form::
    
        http.request(url, body=..., method=...)
                 
    Default ``None``.
     
Lets say your RPC server is running at ``http://domain.name.com/``::
  
    >>> a = JsonProxy('http://domain.name.com/')
    >>> a.add(3,4)
    7
    >>> a.ping()
    'pong'

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

    def timeit(self, func, times, *args, **kwargs):
        '''Usefull little utility for timing responses from server. The
usage is simple::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060')
    >>> p.timeit('ping',10)
    0.56...
    >>> _
    '''
        r = range(times)
        func = getattr(self,func)
        start = default_timer()
        for t in r:
            func(*args, **kwargs)
        return default_timer() - start            
        
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
        resp = self._http.request(self.__url,
                                  method = "POST",
                                  body = body)
        content = resp.content.decode('utf-8')
        if resp.status_code == 200:
            if raw:
                return content
            else:
                return self.loads(content)
        else:
            if 'error' in content:
                return self.loads(content)
            else:
                resp.raise_for_status()
        
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
    