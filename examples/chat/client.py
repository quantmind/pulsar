import datetime

from pulsar import arbiter
from pulsar.apps import rpc, http, ws

import logging
logger=logging.getLogger(__name__)

class ChatRpcClient(object):
    def __init__(self, actor):
        self.actor=actor
        self.proxy = rpc.JsonProxy('http://127.0.0.1:8060/rpc')

    def say_hello(self):
        self.proxy.message('Hi from rpc %s' % datetime.datetime.now()).bind(self.hello_reply)
        self.actor.event_loop.call_later(1, self.say_hello)
    
    def hello_reply(self, result):
        #logger.info('result: %s' % result.decode_content())
        return result

class MessageHandler(ws.WS):
    
    def on_message(self, websocket, message):
        logger.info('on_message %r' % message)

class ChatPubSubClient(object):
    def __init__(self, actor):
        self.actor=actor
        self.http = http.HttpClient()
        
    def receive(self):
        ws = yield self.http.get('ws://127.0.0.1:8060/message',
                                 websocket_handler=MessageHandler()).on_headers
        ws.write('Hello there!')

def start(actor):
    client=ChatRpcClient(actor)
    actor.event_loop.call_soon(client.say_hello)

    pubsub_client=ChatPubSubClient(actor)
    actor.event_loop.call_soon(pubsub_client.receive)
    return actor

def main():
    logging.basicConfig(level=logging.DEBUG)
    arbiter(start=start).start()
    
if __name__=='__main__':
    main()
