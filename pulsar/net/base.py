import socket

import pulsar
from pulsar import Deferred, deferred_timeout

__all__ = ['NetStream',
           'NetRequest',
           'NetResponse',
           'AsyncClientResponse']


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
        
    
class NetRequest(NetStream):
    def __init__(self, stream=None, parsercls=None, **kwargs):
        pcls = parsercls or self.default_parser()
        self.parser = self.get_parser(pcls, **kwargs) if pcls else None
        super(NetRequest,self).__init__(stream, **kwargs)
        
    def default_parser(self):
        return None
        
    def get_parser(self, parsercls, **kwargs):
        return parsercls()
    
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
    
        
class NetResponse(NetStream):
    '''Base class for responses'''
    def __init__(self, request, stream=None, **kwargs):
        self.request = request
        timeout = kwargs.pop('timeout', request.timeout)
        stream = stream or request.stream
        super(NetResponse, self).__init__(stream, timeout=timeout, **kwargs)
    
    def done(self):
        raise NotImplementedError()
    
    @property
    def parser(self):
        return self.request.parser
    
    
class AsyncClientResponse(NetResponse):
    '''A class for managing clients asynchronous response'''
    def __init__(self, *args, **kwargs):
        self.passed = 0
        self._event = Deferred()
        super(AsyncClientResponse, self).__init__(*args, **kwargs)
        
    def read(self):
        return self.parse()
            
    def parse(self, data=None):
        parser = self.parser
        if data is not None:
            self.passed += 1
            try:
                parser.execute(data, len(data))
            except Exception as e:
                self.error(e)
                return
        if not self.done():
            self.stream.read().add_callback(self.parse, self.error)
        else:
            self.finish()
            self._event.callback(self)
        return self._event
    
    def error(self, failure):
        if not self._event.called:
            self._event.callback(failure)
    
    def finish(self):
        pass
    