import socket

import pulsar
from pulsar import Deferred

__all__ = ['NetStream',
           'NetRequest',
           'NetResponse',
           'ClientConnection',
           'ClientResponse']


class NetStream(object):
    '''Wraps an :class:`AsyncIOStream`'''
    def __init__(self, stream=None, timeout=None, **kwargs):
        if stream is None:
            stream = pulsar.AsyncIOStream()
        self.stream = stream
        self.timeout = timeout
        self.events = {}
        self.on_init(kwargs)
    
    def fileno(self):
        return self.stream.fileno()
            
    def close(self):
        '''Close the :class:`NetStream` by calling the
:meth:`on_close` method'''
        yield self.on_close()
        yield self.stream.close()
            
    def on_init(self, kwargs):
        pass
    
    def on_close(self):
        pass
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self.fileno())
    
    def __str__(self):
        return self.__repr__()

    def async_event(self, event_name, callable, *args, **kwargs):
        '''Register an asynchronous event with the event dictionary.
        
:parameter event_name: the name of the event used as key in the event
    dictionary.'''
        return callable(*args, **kwargs).add_callback(
                                lambda r: self.event_callback(event_name, r),
                                lambda r: self.event_errback(event_name, r))
    
    def event_callback(self, event_name, result):
        cbk = getattr(self, 'callback_%s' % event_name, None)
        return cbk(result) if cbk else result
    
    def event_errback(self, event_name, failure):
        cbk = getattr(self, 'errbackk_%' % event_name, None)
        return cbk(failure) if cbk else failure.raise_all()


class ClientConnection(NetStream):
    
    def connect(self, address):
        '''Connect to a remote address.'''
        return self.async_event('connect',
                                self.stream.connect,
                                address)
        
    def send(self, data):
        '''Connect to a remote address.'''
        return self.async_event('write',
                                self.stream.write,
                                data)
    
    
class ClientResponse(NetStream, Deferred):
    
    def __init__(self, request, *args, **kwargs):
        self.request = request
        super(ClientResponse, self).__init__(self, *args, **kwargs)
        Deferred.__init__(self)
        
    
class NetRequest(NetStream):
    def __init__(self, stream, client_addr = None, parsercls = None, **kwargs):
        self.parsercls = parsercls or self.default_parser()
        self.client_address = client_addr
        self.parser = self.get_parser(**kwargs)
        super(NetRequest,self).__init__(stream, **kwargs)
        
    def default_parser(self):
        return None
        
    def get_parser(self, **kwargs):
        if self.parsercls:
            return self.parsercls()
    
        
class NetResponse(NetStream):
    '''Base class for responses'''
    def __init__(self, request, stream=None, **kwargs):
        self.request = request
        timeout = kwargs.pop('timeout', request.timeout)
        stream = stream or request.stream
        super(NetResponse, self).__init__(stream, timeout=timeout, **kwargs)
    
    