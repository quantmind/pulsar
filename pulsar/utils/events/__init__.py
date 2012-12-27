"""Multi-consumer multi-producer dispatching mechanism

Originally based on pydispatch (BSD) http://pypi.python.org/pypi/PyDispatcher/2.0.1
See license.txt for original license.
"""
from .dispatcher import Signal

_events = {}

def create(name, providing_args=None):
    '''Create a new event from a *name*. If the event is already available
it returns it.'''
    name = name.lower()
    if name not in _events:
        event = Signal()
        _events[name] = event
    return _events[name]

def bind(name, callback, sender=None, **kwargs):
    '''Bind a *callback* to event *name*. The optional *sender* can be used
to bind only when the given sender fires the event.'''
    event = create(name)
    event.connect(callback, sender=sender, **kwargs)
    
def fire(name, sender, **kwargs):
    '''Fire event *name* from *sender*.'''
    event = create(name)
    event.send(sender, **kwargs)