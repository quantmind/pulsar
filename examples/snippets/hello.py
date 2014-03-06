'''Write Hello there! every second
'''
from pulsar import arbiter


def hello(actor, **kw):
    print('Hello there!')
    actor._loop.call_later(1, hello, actor)


if __name__ == '__main__':
    arbiter(start=hello).start()
