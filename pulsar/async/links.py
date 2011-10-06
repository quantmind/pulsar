from .defer import Deferred, AlreadyCalledError


class ActorLinkCallback(object):
 
    def __init__(self, link, proxy, sender, action, args, kwargs):
        self.link = link
        self.proxy = proxy
        self.sender = sender
        self.action = action
        self.args = args
        self.kwargs = kwargs
        
    def __call__(self, **kwargs):
        if hasattr(self,'_message'):
            raise AlreadyCalledError()
        self.link.process_middleware(self,kwargs)
        self._message = self.proxy.send(self.sender, self.action,
                                        *self.args, **self.kwargs)
        return self._message


class ActorLink(object):
    '''Utility for sending messages to linked actors.'''
    def __init__(self, name, middleware = None):
        '''Provide a link between two actors.'''
        self.name = name
        self.middleware = middleware or []
        
    def proxy(self, sender):
        proxy = sender.get_actor(self.name)
        if not proxy:
            raise ValueError('Got a request from actor {0} which is\
 not linked with {1}.'.format(sender,self.name))
        return proxy
        
    def add_middleware(self, middleware):
        '''Add a middleware function to the middleware list.
A middleware function takes 2 parameters, an instance of
:class:`ActorLinkCallback` and a dictionary.'''
        if middleware not in self.middleware:
            self.middleware.append(middleware)
    
    def process_middleware(self, callback, kwargs):
        for process in self.middleware:
            try:
                process(callback, kwargs)
            except:
                pass
    
    def get_callback(self, sender, action, *args, **kwargs):
        '''Get an instance of :`ActorLinkCallback`'''
        proxy = self.proxy(sender)
        return ActorLinkCallback(self, proxy, sender, action, args, kwargs)
        
    def __call__(self, sender, action, *args, **kwargs):
        return self.get_callback(sender, action, *args, **kwargs)()
        


    