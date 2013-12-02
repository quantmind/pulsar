from pulsar.apps import ws
from pulsar.apps import http
from pulsar.apps import Application
from pulsar import arbiter

import logging
logger=logging.getLogger(__name__)

class Echo(ws.WS):

    def on_open(self, websocket):
        logger.info('on_open, %r' % websocket)
        self.websocket=websocket

    def on_message(self, websocket, message):
        logger.info('got message: %r' % message)
        websocket.write(message)

    def say_hello(self):
        #self.websocket.write('foooooooo')
        logger.info('say_hello')
        
    def raise_value_error(self):
        # Exception should be visible in the logs
        raise ValueError()

class WebSocketClientApplication(Application):
    def __init__(self, actor):
        self.actor=actor
        from pulsar.apps import rpc
        self.proxy = rpc.JsonProxy('http://127.0.0.1:8060/rpc')

    def say_hello(self):
        self.proxy.message('Hi from rpc').bind(logger.info, Exception)
        self.actor.event_loop.call_later(1, self.say_hello)

def start(actor):
    wsca=WebSocketClientApplication(actor)
    actor.event_loop.call_soon(wsca.say_hello)
    return actor

def main():
    logging.basicConfig(level=logging.DEBUG)
    arbiter(start=start).start()
    
if __name__=='__main__':
    main()
