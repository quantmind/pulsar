from pulsar.apps import ws
from pulsar.apps import http

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
        
def main():
    logging.basicConfig(level=logging.DEBUG)
    client = http.HttpClient()
    echo=Echo()
    ws = client.get('ws://localhost:8060', websocket_handler=echo).on_headers
    logger.info('status_code: %s' % ws.handshake.status_code)
    def init():
        yield echo
    client.event_loop.call_soon(init)
    logger.info('start run_forever()')
    client.event_loop.run_forever()


if __name__=='__main__':
    main()
