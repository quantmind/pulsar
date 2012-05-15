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
    
    @property
    def actor(self):
        return self.stream.actor
            
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
        event = self.events.get(event_name)
        if event is None:
            self.events[event_name] = event = Deferred(description='%s %s'\
                                                        % (self, event_name))
            callable(*args, **kwargs).add_callback(event.callback)
        return event
    

class ClientConnection(NetStream):
    
    def connect(self, address):
        '''Connect to a remote address.'''
        return self.async_event('connect',
                                self.stream.connect,
                                address,
                                callback=self.on_connect,
                                errback=self.on_connect_failure)
    
    def on_connect(self, result):
        return result
    
    def on_connect_failure(self, failure):
        failure.raise_all()
    

class ClientResponse(NetStream, Deferred):
    
    def __init__(self, *args, **kwargs):
        super(NetStream, self).__init__(*args, **kwargs)
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
    
    def __init__(self, request, stream=None, **kwargs):
        self.request = request
        timeout = kwargs.pop('timeout', request.timeout)
        super(NetResponse, self).__init__(stream, timeout=timeout, **kwargs)
    
    