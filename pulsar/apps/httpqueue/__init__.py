'''An HTTP message queue implementation.'''
from collections import deque

import pulsar
from pulsar import get_actor, send
from pulsar.apps.http import HttpClient
from pulsar.apps import wsgi, ws


class QueueManager(object):
    '''The :class:`QueueManager` is responsible for the actual queue in the
queue server. It is a wsgi callable object which add itself to the WSGI environ
so that workers can poll data from it.'''
    protocol_factory = None
    
    def put(self, message):
        '''Put a new *message* into the queue.'''
        raise NotImplementedError
    
    def poll(self, client):
        '''Poll a message from the queue if possible.'''
        raise NotImplementedError
    
    def size(self):
        '''Size of the queue.'''
        raise NotImplementedError
    
    def __call__(self, environ, start_response):
        # Add self to the WSGI environ
        environ['queue.manager'] = self
    

class QueueMiddleware(wsgi.Router):
    
    def post(self, request):
        '''Add a new task to the queue.'''
        data = request.environ['wsgi.input'].read()
        queue = environ['queue.manager']
        queue.put(data)
        request.response.status_code = 200
        request.response.content_type = 'text/plain'
        request.response.content = '%s' % queue.size()
        

class QueueSocket(ws.WS):
        
    def on_message(self, environ, msg):
        if msg == 'poll':
            queue = environ['queue.manager']
            return queue.poll(environ['pulsar.connection'])
        

class QueueClient(object):
    
    def __init__(self, url, http=None, **kw):
        self.__url = url
        self.http = HttpClient(**kw)

    @property
    def url(self):
        return self.__url
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__url)

    def __str__(self):
        return self.__repr__()
    
    def put(self, data):
        return self.http.post(self.url, data=data)
    
    def poll(self):
        return self.http.get(self.url)

################################################################################
## Queue Manager pulsar implementation
@pulsar.command()
def poll_queue_message(request):
    caller = request.caller
    queue = get_queue()
    return queue.poll()


class PulsarQueue(QueueManager):
    '''A :class:`QueueManager` implementation which keeps waiting workers
into a deque.'''
    def __init__(self, name):
        self._name = name
        self._pollers = deque()
        self._queue = deque()
        
    def poll(self, client):
        '''Call by a client when it needs to poll data from the queue.'''
        send(self._name, 'poll_message').add_callback(
                                        partial(self._send_data, client))
        
    def put(self, data):
        self._queue.append(data)
        self._send_data()
        
    def size(self):
        return len(self._queue)
        
    def _send_data(self, client, data):
        if data is not None:
            client.write(data)
        else:
            self._pollers.append(client)
    
    
def server(name='message_queue', queue_middleware=None,
            queue_manager=None, **kwargs):
    if queue_manager is None:
        queue_manager = PulsarQueue(name)
    if queue_middleware is None:
        queue_middleware = QueueMiddleware('/')
    websocket = ws.WebSocket('/messages', QueueSocket)
    middleware = [queue_manager, websocket, queue_middleware]
    return wsgi.WSGIServer(callable=middleware, name=name, **kwargs)
