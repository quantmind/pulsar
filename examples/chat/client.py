import datetime

from pulsar import arbiter

import logging
logger=logging.getLogger(__name__)

class ChatClient(object):
    def __init__(self, actor):
        self.actor=actor
        from pulsar.apps import rpc
        self.proxy = rpc.JsonProxy('http://127.0.0.1:8060/rpc')

    def say_hello(self):
        self.proxy.message('Hi from rpc %s' % datetime.datetime.now()).bind(logger.info, Exception)
        self.actor.event_loop.call_later(1, self.say_hello)

def start(actor):
    client=ChatClient(actor)
    actor.event_loop.call_soon(client.say_hello)
    return actor

def main():
    logging.basicConfig(level=logging.DEBUG)
    arbiter(start=start).start()
    
if __name__=='__main__':
    main()
