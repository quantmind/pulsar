'''Write Hello there! every second
'''
try:
    import pulsar
except ImportError:     # pragma nocover
    import sys
    sys.path.append('../../')
    import pulsar

from pulsar import arbiter


def hello(actor):
    print('Hello there!')
    actor._loop.call_later(1, hello, actor)
    return actor


if __name__ == '__main__':
    arbiter(start=hello).start()
