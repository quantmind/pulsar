'''Micro observer-observable (event) library'''
import logging

logger = logging.getLogger('plusar.event')
_events=  {}

class Event(object):
    '''An event is created using the :func:`create` function.'''
    def __init__(self, name):
        self.name = name
        self.observers = []
    
    def trigger(self, value, sender):
        observers = self.observers[:]
        self.observers = []
        self.observers.extend(self._notify(observers, value, sender))
        
    def _notify(self, observers, value, sender):
        for observer in observers:
            observer.notify(value, sender=sender)
            if not observer.once_only:
                yield observer
            
            
class Observer(object):
    '''get notified by :class:`Event`'''
    def __init__(self, callback, once_only):
        self.callback = callback
        self.once_only = once_only
        
    def notify(self, value, sender=None):
        '''The observer get notified'''
        try:
            self.callback(value=value, sender=sender)
        except:
            logger.error('Unhandled exception in event handler', exc_info=True)
        
        
def create(name):
    '''Create a new event from a *name*. If the event is already available
it returns it.'''
    name = name.lower()
    if name not in _events:
        event = Event(name)
        _events[name] = event
    return _events[name]
    
def bind(name, callback, once_only=False):
    '''Bind a *callback* to event *name*. If *once_only* is ``True``
the callback will be invoked only once and than removed from the event
handler.'''
    event = create(name)
    observer = Observer(callback, once_only)
    event.observers.append(observer)
    
def fire(name, value=None, sender=None):
    event = create(name)
    event.trigger(value, sender)