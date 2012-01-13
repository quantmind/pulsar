from .defer import Deferred, AlreadyCalledError, make_async


class LocalData(object):
    
    @property
    def local(self):
        if not hasattr(self,'_local'):
            self._local = {}
        return self._local


class ActorLinkCallback(LocalData):
    '''Instances of this class are created by the
:meth:`ActorLink.get_callback` method. This is a callable object
which can be called back once only otherwise a :class:`AlreadyCalledError`
exception will raise.'''
    def __init__(self, link, proxy, sender, action, args, kwargs):
        self.link = link
        self.proxy = proxy
        self.sender = sender
        self.action = action
        self.args = args
        self.kwargs = kwargs
        
    def __call__(self, *args, **kwargs):
        if not hasattr(self,'_message'):
            self.args += args
            self.kwargs.update(kwargs)
            msg = self.proxy.send(self.sender, self.action,
                                  *self.args, **self.kwargs)
            self._message = make_async(msg)
        else:
            raise AlreadyCalledError('Already called')
        return self._message
    
    def result(self):
        '''Call this function to get a result. If self was never called
return a :class:`Deferred` already called with ``None``.'''
        if not hasattr(self,'_message'):
            d = Deferred()
            self._message = d
            d.callback(None)
        return self._message
        

class ActorLink(object):
    '''A callable utility for sending :class:`ActorMessage`
to linked :class:`Actor` instances.
    
.. attribute:: name

    The :attr:`Actor.name` of the actor which will receive messages via
    the :class:`ActorLink` from other actors.
    
An example on how to use an :class:`ActorLink` can be found in the
:class:`pulsar.apps.tasks.TaskQueueRpcMixin`, where the
``task_queue_manager`` attribute is a lint to the
:class:`pulsar.apps.tasks.TaskQueue`. 
'''
    def __init__(self, name):
        self.name = name
        
    def proxy(self, sender):
        '''Get the :class:`ActorProxy` for the sender.'''
        proxy = sender.get_actor(self.name)
        if not proxy:
            raise ValueError('Got a request from actor "{0}" which is\
 not linked with "{1}".'.format(sender,self.name))
        return proxy
    
    def get_callback(self, sender, action, *args, **kwargs):
        '''Create an :class:`ActorLinkCallback` for sending messages
with additional parameters.

:parameter sender: The :class:`Actor` sending the message.
:parameter action: The *action* in the :class:`ActorMessage`.
:parameter args: same as :attr:`ActorMessage.args`
:parameter kwargs: same as :attr:`ActorMessage.kwargs`
:rtype: an :class:`ActorLinkCallback`.
'''
        if isinstance(sender,dict):
            # This is an environment dictionary
            local = sender
            sender = sender.get('pulsar.actor')
        else:
            local = kwargs.pop('local',None)
        proxy = self.proxy(sender)
        res = ActorLinkCallback(self, proxy, sender, action, args, kwargs)
        if local:
            res._local = local
        return res
        
    def __call__(self, sender, action, *args, **kwargs):
        return self.get_callback(sender, action, *args, **kwargs)()
        


    