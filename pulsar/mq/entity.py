TRANSIENT_DELIVERY_MODE = 1
PERSISTENT_DELIVERY_MODE = 2
DELIVERY_MODES = {"transient": TRANSIENT_DELIVERY_MODE,
                  "persistent": PERSISTENT_DELIVERY_MODE}
ACKNOWLEDGED_STATES = ("ACK", "REJECTED", "REQUEUED")
MESSAGE_STATES = ('CREATED','RECEIVED') + ACKNOWLEDGED_STATES

EMPTY_DICT = {}


class MQ(object):
    '''A mixin for all :mod:`pulsar.mq` elements.'''
    pass


# The basic entities

class Container(MQ):
    '''A pulsar mq Container is a specialised :class:`pulsar.Monitor`'''
    
    def register_node(self, node):
        '''Register a :class:`pulsar.mq.Node` with the container.'''
        raise NotImplementedError


class Node(MQ):
    '''A node in the message queue network.
    
.. attribute: name
    
    the unique name of the node
'''
    _name = None
    
    def __init__(self, name):
        self._name = name
        
    @property
    def name(self):
        return self._name


class Link(MQ):
    pass


# Two types of containers

class Client(Container):
    pass


class Broker(Container):
    pass





