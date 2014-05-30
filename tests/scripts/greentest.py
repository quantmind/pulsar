from pulsar import arbiter, get_event_loop
from pulsar.apps.greenio import wait, greenlet


def example(loop):
    wait(callback)
    loop.stop()


if __name__ == '__main__':
    a = arbiter()
    loop = a._loop
    loop.call_soon(example, loop)
    a.start()
